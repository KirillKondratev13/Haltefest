import asyncio
import hashlib
import json
import logging
import os
import re
import signal
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

import aiohttp
import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from fastembed import TextEmbedding

TOPIC_IN = os.getenv("KAFKA_TEXT_TO_INDEX_TOPIC", "text-to-index")
TOPIC_DLQ = os.getenv("KAFKA_ANALYSIS_DLQ_TOPIC", "analysis-dlq")
KAFKA_BROKER = os.getenv("KAFKA_BROKERS", os.getenv("KAFKA_BROKER", "kafka:9092"))
POSTGRES_URL = os.getenv(
    "DB_CONN_STR",
    os.getenv("POSTGRES_URL", "postgresql://myappuser:mypassword@postgres:5432/myapp"),
)
SEAWEED_URL = os.getenv("FILER_URL", os.getenv("SEAWEED_URL", "http://seaweedfs-filer:8888"))
CONSUMER_GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP", "indexer_service_group")
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "6"))
MAX_ERROR_MESSAGE_LEN = 1200
RETRY_BACKOFF_SECONDS = [1, 5, 15, 60, 300, 900]
KAFKA_STARTUP_RETRY_SECONDS = int(os.getenv("KAFKA_STARTUP_RETRY_SECONDS", "3"))

QDRANT_URL = os.getenv("QDRANT_URL", "http://qdrant:6333")
EMBEDDING_MODEL_NAME = os.getenv("EMBEDDING_MODEL", "BAAI/bge-small-en-v1.5")
EMBEDDING_MODEL_VERSION = os.getenv("EMBEDDING_MODEL_VERSION", "bge-small-en-v1.5")
CHUNKING_VERSION = os.getenv("CHUNKING_VERSION", "chunking-v1")
VECTOR_SIZE = 384
COLLECTION_NAME = "file_chunks"
TARGET_CHUNK_TOKENS = 500
OVERLAP_TOKENS = 80

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("IndexerService")


class TerminalError(Exception):
    pass


async def start_with_kafka_retry(start_fn, component_name: str, stop_event: asyncio.Event) -> bool:
    while not stop_event.is_set():
        try:
            await start_fn()
            return True
        except KafkaConnectionError as err:
            logger.warning(
                "kafka bootstrap not ready, retrying startup",
                extra={
                    "component": component_name,
                    "kafka": KAFKA_BROKER,
                    "retry_in_sec": KAFKA_STARTUP_RETRY_SECONDS,
                    "error": str(err),
                },
            )
            await asyncio.sleep(KAFKA_STARTUP_RETRY_SECONDS)
    return False


@dataclass
class IndexEvent:
    event_id: str
    file_id: int
    user_id: int
    s3_text_path: str
    text_version: int
    parser_version: str
    created_at: str

    def to_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "file_id": self.file_id,
            "user_id": self.user_id,
            "s3_text_path": self.s3_text_path,
            "text_version": self.text_version,
            "parser_version": self.parser_version,
            "created_at": self.created_at,
        }


@dataclass
class Chunk:
    chunk_id: int
    chunk_text: str
    char_start: int
    char_end: int


def parse_event(raw_value: bytes) -> IndexEvent:
    payload = json.loads(raw_value.decode("utf-8"))
    return IndexEvent(
        event_id=str(payload["event_id"]),
        file_id=int(payload["file_id"]),
        user_id=int(payload["user_id"]),
        s3_text_path=str(payload["s3_text_path"]),
        text_version=int(payload["text_version"]),
        parser_version=str(payload["parser_version"]),
        created_at=str(payload["created_at"]),
    )


def build_seaweed_url(path: str) -> str:
    normalized_path = path if path.startswith("/") else f"/{path}"
    return f"{SEAWEED_URL.rstrip('/')}{normalized_path}"


def sanitize_error_message(err: Exception) -> str:
    message = f"{err.__class__.__name__}: {err}".replace("\n", " ").strip()
    if not message:
        message = "unknown indexer error"
    if len(message) <= MAX_ERROR_MESSAGE_LEN:
        return message
    return f"{message[:MAX_ERROR_MESSAGE_LEN]} ... [truncated]"


def get_retry_delay(attempt: int) -> int:
    idx = max(0, min(attempt - 1, len(RETRY_BACKOFF_SECONDS) - 1))
    return RETRY_BACKOFF_SECONDS[idx]


def now_rfc3339() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def point_id_from_parts(file_id: int, text_version: int, chunk_id: int) -> str:
    raw = f"{file_id}:{text_version}:{chunk_id}"
    h = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]
    return str(uuid.UUID(h))


def normalize_whitespace(text: str) -> str:
    text = re.sub(r"\r\n?", "\n", text)
    text = re.sub(r"\t", " ", text)
    text = re.sub(r" {2,}", " ", text)
    lines = [line.strip() for line in text.splitlines()]
    return "\n".join(line for line in lines if line)


def estimate_tokens(text: str) -> int:
    return max(1, len(text.split()))


def chunk_text(text: str) -> list[Chunk]:
    normalized = normalize_whitespace(text)
    if not normalized:
        return []

    heading_pattern = re.compile(r"^(\d+(\.\d+)*\s+.+|[A-Z\u0410-\u042f\u0401]{2,}(\s+[A-Z\u0410-\u042f\u0401]{2,})*)$", re.MULTILINE)

    segments: list[tuple[str, int, int]] = []
    last_end = 0

    positions = [(m.start(), m.end()) for m in heading_pattern.finditer(normalized)]

    if not positions:
        paragraphs = re.split(r"\n\s*\n", normalized)
        pos = 0
        for para in paragraphs:
            para = para.strip()
            if para:
                start = normalized.index(para, pos)
                segments.append((para, start, start + len(para)))
                pos = start + len(para)
    else:
        seg_start = 0
        for heading_start, heading_end in positions:
            if heading_start > seg_start:
                block = normalized[seg_start:heading_start].strip()
                if block:
                    for para in re.split(r"\n\s*\n", block):
                        para = para.strip()
                        if para:
                            ps = normalized.index(para, seg_start)
                            segments.append((para, ps, ps + len(para)))
            heading_line = normalized[heading_start:heading_end].strip()
            if heading_line:
                segments.append((heading_line, heading_start, heading_end))
            seg_start = heading_end

        if seg_start < len(normalized):
            remaining = normalized[seg_start:].strip()
            if remaining:
                for para in re.split(r"\n\s*\n", remaining):
                    para = para.strip()
                    if para:
                        ps = normalized.index(para, seg_start)
                        segments.append((para, ps, ps + len(para)))

    chunks: list[Chunk] = []
    chunk_id = 0
    i = 0

    while i < len(segments):
        current_texts: list[str] = []
        current_start = segments[i][1]
        current_end = segments[i][2]
        token_count = 0

        while i < len(segments) and token_count < TARGET_CHUNK_TOKENS:
            seg_text, seg_start, seg_end = segments[i]
            seg_tokens = estimate_tokens(seg_text)

            if token_count + seg_tokens > TARGET_CHUNK_TOKENS and token_count > 0:
                break

            current_texts.append(seg_text)
            current_end = seg_end
            token_count += seg_tokens
            i += 1

        if not current_texts:
            i += 1
            continue

        chunk_id += 1
        chunks.append(Chunk(
            chunk_id=chunk_id,
            chunk_text="\n".join(current_texts),
            char_start=current_start,
            char_end=current_end,
        ))

        if OVERLAP_TOKENS > 0 and i < len(segments):
            overlap_texts: list[str] = []
            overlap_tokens = 0
            j = len(current_texts) - 1
            while j >= 0 and overlap_tokens < OVERLAP_TOKENS:
                overlap_texts.insert(0, current_texts[j])
                overlap_tokens += estimate_tokens(current_texts[j])
                j -= 1

    if not chunks and normalized:
        chunks.append(Chunk(
            chunk_id=1,
            chunk_text=normalized,
            char_start=0,
            char_end=len(normalized),
        ))

    return chunks


async def download_text(session: aiohttp.ClientSession, s3_text_path: str) -> str:
    url = build_seaweed_url(s3_text_path)
    async with session.get(url) as response:
        if response.status != 200:
            body = await response.text()
            raise RuntimeError(f"s3 download failed status={response.status}, url={url}, body={body[:300]}")
        return (await response.text()).strip()


async def mark_processing(db: asyncpg.Connection, event: IndexEvent) -> str:
    row = await db.fetchrow(
        """
        SELECT index_status, user_id
        FROM text_artifacts
        WHERE file_id = $1 AND text_version = $2
        FOR UPDATE
        """,
        event.file_id,
        event.text_version,
    )
    if row is None:
        raise TerminalError(f"text_artifact not found: file_id={event.file_id}, text_version={event.text_version}")
    if int(row["user_id"]) != event.user_id:
        raise TerminalError(
            f"user mismatch for text_artifact: file_id={event.file_id}, text_version={event.text_version}, event_user={event.user_id}"
        )

    current_status = str(row["index_status"] or "").upper()
    if current_status == "DONE":
        return "DONE"

    await db.execute(
        """
        UPDATE text_artifacts
        SET index_status = 'PROCESSING', index_error = NULL
        WHERE file_id = $1 AND text_version = $2
        """,
        event.file_id,
        event.text_version,
    )
    return "PROCESSING"


async def mark_done(db: asyncpg.Connection, event: IndexEvent) -> None:
    await db.execute(
        """
        UPDATE text_artifacts
        SET index_status = 'DONE', index_error = NULL, indexed_at = NOW()
        WHERE file_id = $1 AND text_version = $2
        """,
        event.file_id,
        event.text_version,
    )


async def mark_failed(db: asyncpg.Connection, event: IndexEvent, error_message: str) -> None:
    await db.execute(
        """
        UPDATE text_artifacts
        SET index_status = 'FAILED', index_error = $3
        WHERE file_id = $1 AND text_version = $2
        """,
        event.file_id,
        event.text_version,
        error_message,
    )


async def publish_dlq(
    producer: AIOKafkaProducer,
    event: IndexEvent,
    error_code: str,
    error_message: str,
    attempt: int,
) -> None:
    payload = {
        "event_id": event.event_id,
        "source_topic": TOPIC_IN,
        "source_payload": event.to_dict(),
        "error_code": error_code,
        "error_message": error_message,
        "attempt": attempt,
        "failed_at": now_rfc3339(),
    }
    await producer.send_and_wait(TOPIC_DLQ, json.dumps(payload).encode("utf-8"))


def ensure_collection(qdrant: QdrantClient) -> None:
    collections = [c.name for c in qdrant.get_collections().collections]
    if COLLECTION_NAME not in collections:
        qdrant.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(size=VECTOR_SIZE, distance=Distance.COSINE),
        )
        logger.info("qdrant collection created", extra={"collection": COLLECTION_NAME, "vector_size": VECTOR_SIZE})
    else:
        logger.info("qdrant collection exists", extra={"collection": COLLECTION_NAME})


def embed_chunks(embedder: TextEmbedding, texts: list[str]) -> list[list[float]]:
    embeddings = list(embedder.embed(texts))
    return [list(e) for e in embeddings]


def upsert_to_qdrant(
    qdrant: QdrantClient,
    event: IndexEvent,
    chunks: list[Chunk],
    embeddings: list[list[float]],
) -> None:
    points: list[PointStruct] = []
    created_at = now_rfc3339()

    for chunk, embedding in zip(chunks, embeddings):
        point_id = point_id_from_parts(event.file_id, event.text_version, chunk.chunk_id)
        payload = {
            "user_id": event.user_id,
            "file_id": event.file_id,
            "text_version": event.text_version,
            "chunk_id": chunk.chunk_id,
            "chunk_text": chunk.chunk_text,
            "char_start": chunk.char_start,
            "char_end": chunk.char_end,
            "chunking_version": CHUNKING_VERSION,
            "embedding_model_version": EMBEDDING_MODEL_VERSION,
            "created_at": created_at,
        }
        points.append(PointStruct(
            id=point_id,
            vector=embedding,
            payload=payload,
        ))

    qdrant.upsert(collection_name=COLLECTION_NAME, points=points)


async def process_event(
    event: IndexEvent,
    db: asyncpg.Connection,
    session: aiohttp.ClientSession,
    producer: AIOKafkaProducer,
    qdrant: QdrantClient,
    embedder: TextEmbedding,
) -> None:
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            async with db.transaction():
                state = await mark_processing(db, event)
            if state == "DONE":
                logger.info(
                    "text already indexed",
                    extra={"event_id": event.event_id, "file_id": event.file_id, "text_version": event.text_version},
                )
                return

            text = await download_text(session, event.s3_text_path)
            if not text:
                raise ValueError("canonical text is empty")

            chunks = chunk_text(text)
            if not chunks:
                raise ValueError("chunking produced no chunks")

            chunk_texts = [c.chunk_text for c in chunks]
            embeddings = await asyncio.to_thread(embed_chunks, embedder, chunk_texts)

            await asyncio.to_thread(upsert_to_qdrant, qdrant, event, chunks, embeddings)

            await mark_done(db, event)

            logger.info(
                "indexing completed",
                extra={
                    "event_id": event.event_id,
                    "file_id": event.file_id,
                    "text_version": event.text_version,
                    "chunk_count": len(chunks),
                },
            )
            return
        except TerminalError as err:
            safe_error = sanitize_error_message(err)
            await mark_failed(db, event, safe_error)
            await publish_dlq(producer, event, "INDEX_TERMINAL_ERROR", safe_error, attempt)
            logger.error(
                "indexing terminal failure",
                extra={"event_id": event.event_id, "file_id": event.file_id, "error": safe_error, "attempt": attempt},
            )
            return
        except Exception as err:  # noqa: BLE001
            safe_error = sanitize_error_message(err)
            if attempt >= MAX_ATTEMPTS:
                await mark_failed(db, event, safe_error)
                await publish_dlq(producer, event, "INDEX_MAX_ATTEMPTS", safe_error, attempt)
                logger.error(
                    "indexing failed after retries",
                    extra={
                        "event_id": event.event_id,
                        "file_id": event.file_id,
                        "attempt": attempt,
                        "error": safe_error,
                    },
                )
                return

            delay = get_retry_delay(attempt)
            logger.warning(
                "indexing retry scheduled",
                extra={"event_id": event.event_id, "file_id": event.file_id, "attempt": attempt, "retry_in_sec": delay},
            )
            await asyncio.sleep(delay)


async def main() -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_args: stop_event.set())

    consumer = AIOKafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=KAFKA_BROKER,
        group_id=CONSUMER_GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)
    db: asyncpg.Connection | None = None
    session: aiohttp.ClientSession | None = None
    consumer_started = False
    producer_started = False

    try:
        db = await asyncpg.connect(POSTGRES_URL)
        session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120))

        qdrant = QdrantClient(url=QDRANT_URL, check_compatibility=False)
        ensure_collection(qdrant)

        embedder = TextEmbedding(EMBEDDING_MODEL_NAME)

        consumer_started = await start_with_kafka_retry(consumer.start, "indexer-consumer", stop_event)
        producer_started = await start_with_kafka_retry(producer.start, "indexer-producer", stop_event)
        if not consumer_started or not producer_started:
            return

        logger.info(
            "indexer service started",
            extra={
                "topic_in": TOPIC_IN,
                "topic_dlq": TOPIC_DLQ,
                "kafka": KAFKA_BROKER,
                "db": POSTGRES_URL,
                "seaweed": SEAWEED_URL,
                "qdrant": QDRANT_URL,
                "embedding_model": EMBEDDING_MODEL_NAME,
            },
        )

        while not stop_event.is_set():
            batches = await consumer.getmany(timeout_ms=1000, max_records=1)
            if not batches:
                continue

            for _partition, messages in batches.items():
                for message in messages:
                    try:
                        event = parse_event(message.value)
                    except Exception as err:  # noqa: BLE001
                        logger.exception("invalid text-to-index payload", extra={"error": str(err)})
                        await consumer.commit()
                        continue

                    await process_event(event, db, session, producer, qdrant, embedder)
                    await consumer.commit()
    finally:
        logger.info("indexer service stopping")
        if consumer_started:
            await consumer.stop()
        if producer_started:
            await producer.stop()
        if session is not None and not session.closed:
            await session.close()
        if db is not None:
            await db.close()
        logger.info("indexer service stopped")


if __name__ == "__main__":
    asyncio.run(main())
