import json
import logging
import os
import signal
import time
from pathlib import Path
from typing import Callable, TypeVar

import psycopg2
import redis
import torch
import torch.nn.functional as F
from confluent_kafka import Consumer, KafkaError
from transformers import AutoModel, AutoTokenizer

T = TypeVar("T")

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("MLService")

# Config from env
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
DB_CONN_STR = os.getenv("DB_CONN_STR", "postgres://myappuser:mypassword@localhost:5432/myapp")
MODEL_NAME = os.getenv("MODEL_NAME", "chandar-lab/NeoBERT")
TAGS_FILE = os.getenv("TAGS_FILE", "tags.txt")
TOPIC_NAME = "text-to-classify"
MAX_RETRIES = 3
INITIAL_BACKOFF_SECONDS = 0.5
MAX_TOKENS = 4096


def retry_with_backoff(operation_name: str, operation: Callable[[], T]) -> T:
    delay = INITIAL_BACKOFF_SECONDS
    last_error: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return operation()
        except Exception as err:  # noqa: BLE001
            last_error = err
            if attempt == MAX_RETRIES:
                break

            logger.warning(
                "%s failed, retrying",
                operation_name,
                extra={"attempt": attempt, "max_attempts": MAX_RETRIES, "retry_in_sec": delay},
            )
            time.sleep(delay)
            delay *= 2

    assert last_error is not None
    raise last_error


def read_tags(path: str) -> list[str]:
    tags_path = Path(path)
    if not tags_path.exists():
        raise FileNotFoundError(f"TAGS_FILE not found: {path}")

    tags = [line.strip() for line in tags_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not tags:
        raise ValueError(f"TAGS_FILE is empty: {path}")
    return tags


class NeoBERTTagger:
    def __init__(self, model_name: str, tags: list[str]) -> None:
        self.model_name = model_name
        self.tags = tags

        logger.info("Loading NeoBERT model", extra={"model_name": model_name, "tags_count": len(tags)})
        self.tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True)
        self.model = AutoModel.from_pretrained(model_name, trust_remote_code=True)
        self.model.eval()
        self.tag_embeddings = self._embed_texts(tags)
        logger.info("NeoBERT model loaded")

    def _embed_texts(self, texts: list[str]) -> torch.Tensor:
        inputs = self.tokenizer(
            texts,
            return_tensors="pt",
            truncation=True,
            padding=True,
            max_length=MAX_TOKENS,
        )
        with torch.no_grad():
            outputs = self.model(**inputs)

        embeddings = outputs.last_hidden_state[:, 0, :]
        return F.normalize(embeddings, p=2, dim=1)

    def predict(self, text: str) -> tuple[str, float]:
        doc_embedding = self._embed_texts([text])
        similarities = torch.matmul(doc_embedding, self.tag_embeddings.T).squeeze(0)
        best_idx = int(torch.argmax(similarities).item())
        return self.tags[best_idx], float(similarities[best_idx].item())


def mark_as_ready(conn: psycopg2.extensions.connection, file_id: int, tag: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE files
            SET tag = %s, status = 'READY', failure_cause = NULL
            WHERE id = %s
            """,
            (tag, file_id),
        )


def mark_as_error(conn: psycopg2.extensions.connection, file_id: int, failure_cause: str) -> None:
    with conn.cursor() as cur:
        # Do not overwrite READY files on duplicate message delivery.
        cur.execute(
            """
            UPDATE files
            SET status = 'ERROR', failure_cause = %s
            WHERE id = %s AND status <> 'READY'
            """,
            (failure_cause, file_id),
        )


def process_message(
    redis_client: redis.Redis,
    db_conn: psycopg2.extensions.connection,
    tagger: NeoBERTTagger,
    msg_value: bytes,
) -> bool:
    try:
        payload = json.loads(msg_value.decode("utf-8"))
    except json.JSONDecodeError as err:
        logger.error("Invalid Kafka payload JSON", extra={"error": str(err)})
        return True

    file_id = payload.get("file_id")
    cache_key = payload.get("cache_key")
    request_id = payload.get("request_id")

    if not file_id or not cache_key:
        logger.error("Invalid payload schema", extra={"payload": payload})
        return True

    log_ctx = {"file_id": file_id, "cache_key": cache_key}
    if request_id:
        log_ctx["request_id"] = request_id

    logger.info("Processing classification request", extra=log_ctx)

    try:
        text = retry_with_backoff("Redis GET text", lambda: redis_client.get(cache_key))
    except Exception as err:  # noqa: BLE001
        logger.error("Failed to get text from cache", extra={**log_ctx, "error": str(err)})
        return False

    if not text:
        logger.warning("Text is missing in cache", extra=log_ctx)
        try:
            retry_with_backoff(
                "Postgres mark error (missing cache)",
                lambda: mark_as_error(db_conn, file_id, "Text not found in cache (expired or missing)"),
            )
        except Exception as err:  # noqa: BLE001
            logger.error("Failed to persist missing-cache error", extra={**log_ctx, "error": str(err)})
            return False
        return True

    try:
        predicted_tag, score = tagger.predict(text)
        logger.info(
            "Inference completed",
            extra={**log_ctx, "predicted_tag": predicted_tag, "similarity_score": round(score, 4)},
        )
    except Exception as err:  # noqa: BLE001
        logger.error("Inference failed", extra={**log_ctx, "error": str(err)})
        try:
            retry_with_backoff(
                "Postgres mark error (inference failed)",
                lambda: mark_as_error(db_conn, file_id, f"Inference failed: {err}"),
            )
        except Exception as db_err:  # noqa: BLE001
            logger.error("Failed to persist inference error", extra={**log_ctx, "error": str(db_err)})
            return False

        try:
            retry_with_backoff("Redis DEL after inference error", lambda: redis_client.delete(cache_key))
        except Exception as redis_err:  # noqa: BLE001
            logger.error("Failed to cleanup cache after inference error", extra={**log_ctx, "error": str(redis_err)})
        return True

    try:
        retry_with_backoff(
            "Postgres mark ready",
            lambda: mark_as_ready(db_conn, file_id, predicted_tag),
        )
    except Exception as err:  # noqa: BLE001
        logger.error("Failed to persist classification result", extra={**log_ctx, "error": str(err)})
        return False

    try:
        retry_with_backoff("Redis DEL after success", lambda: redis_client.delete(cache_key))
    except Exception as err:  # noqa: BLE001
        logger.error("Failed to cleanup cache after success", extra={**log_ctx, "error": str(err)})

    logger.info("Message processed successfully", extra=log_ctx)
    return True


def connect_redis(url: str) -> redis.Redis:
    client = redis.from_url(url, decode_responses=True)
    client.ping()
    return client


def main() -> None:
    logger.info("ML Service starting...")
    tags = read_tags(TAGS_FILE)
    tagger = NeoBERTTagger(MODEL_NAME, tags)

    redis_client = retry_with_backoff(
        "Redis connection",
        lambda: connect_redis(REDIS_URL),
    )

    db_conn = retry_with_backoff(
        "Postgres connection",
        lambda: psycopg2.connect(DB_CONN_STR),
    )
    db_conn.autocommit = True

    consumer_conf = {
        "bootstrap.servers": KAFKA_BROKERS,
        "group.id": "ml_service_group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC_NAME])

    shutdown_requested = False

    def handle_shutdown(signum: int, _frame: object) -> None:
        nonlocal shutdown_requested
        logger.info("Signal received, shutting down", extra={"signal": signum})
        shutdown_requested = True

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    logger.info(
        "ML Service active",
        extra={"brokers": KAFKA_BROKERS, "topic": TOPIC_NAME, "tags_file": TAGS_FILE, "model_name": MODEL_NAME},
    )

    try:
        while not shutdown_requested:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Kafka consumer error", extra={"error": str(msg.error())})
                continue

            processed = process_message(redis_client, db_conn, tagger, msg.value())
            if not processed:
                logger.warning("Message processing incomplete, offset will not be committed yet")
                continue

            try:
                retry_with_backoff(
                    "Kafka commit",
                    lambda: consumer.commit(message=msg, asynchronous=False),
                )
            except Exception as err:  # noqa: BLE001
                logger.error("Failed to commit Kafka offset", extra={"error": str(err)})

    finally:
        consumer.close()
        db_conn.close()
        logger.info("ML Service stopped")


if __name__ == "__main__":
    main()
