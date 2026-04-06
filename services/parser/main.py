import asyncio
import ast
import hashlib
import io
import json
import logging
import os
import re
import signal
import time
import uuid
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import PurePosixPath

import aiohttp
import asyncpg
import docx
import pypdf
import redis.asyncio as redis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

TOPIC_IN = os.getenv("TOPIC_IN", "files-to-parse")
TOPIC_OUT = os.getenv("TOPIC_OUT", "text-to-classify")
KAFKA_BROKER = os.getenv("KAFKA_BROKERS", os.getenv("KAFKA_BROKER", "kafka:9092"))
REDIS_URL = os.getenv("REDIS_URL", "redis://dragonfly:6379")
POSTGRES_URL = os.getenv("DB_CONN_STR", os.getenv("POSTGRES_URL", "postgresql://myappuser:mypassword@postgres:5432/myapp"))
SEAWEED_URL = os.getenv("FILER_URL", os.getenv("SEAWEED_URL", "http://seaweedfs-filer:8888"))
PARSER_VERSION = os.getenv("PARSER_VERSION", "parser-1.0.0")
TEXT_CANONICAL_PREFIX = os.getenv("TEXT_CANONICAL_PREFIX", "/texts")

MAX_FILE_SIZE_BYTES = 25 * 1024 * 1024
MIN_TEXT_LEN = 50
TEXT_TTL_SECONDS = 86400
CONSUMER_GROUP_ID = "parser-service-group"
MAX_ERROR_MESSAGE_LEN = 1200
ARTIFACT_WRITE_MAX_ATTEMPTS = 3
TEXT_ENCODINGS = ("utf-8", "utf-8-sig", "cp1251", "koi8-r", "cp866")
BYTES_LITERAL_RE = re.compile(r"""^b(['"]).*\1$""", re.DOTALL)
HEX_ESCAPE_RUN_RE = re.compile(r"(?:\\x[0-9a-fA-F]{2}){3,}")
USER_PATH_RE = re.compile(r"^/user_(\d+)/")

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("ParserService")


@dataclass
class FileMessage:
    file_id: int
    user_id: int
    s3_path: str
    mime_type: str


@dataclass
class ClassifyMessage:
    file_id: int
    cache_key: str
    request_id: str


def parse_message(raw_value: bytes) -> FileMessage:
    payload = json.loads(raw_value.decode("utf-8"))
    s3_path = str(payload["s3_path"])
    user_id_raw = payload.get("user_id")

    if user_id_raw is None:
        # Backward-compatible fallback for old producer payloads.
        normalized_path = s3_path if s3_path.startswith("/") else f"/{s3_path}"
        match = USER_PATH_RE.match(normalized_path)
        if match is None:
            raise ValueError("missing required field user_id")
        user_id_raw = match.group(1)

    return FileMessage(
        file_id=int(payload["file_id"]),
        user_id=int(user_id_raw),
        s3_path=s3_path,
        mime_type=str(payload.get("mime_type", "")),
    )


def build_seaweed_url(path: str) -> str:
    normalized_path = path if path.startswith("/") else f"/{path}"
    return f"{SEAWEED_URL.rstrip('/')}{normalized_path}"


def build_canonical_text_path(user_id: int, file_id: int, text_version: int) -> str:
    prefix = TEXT_CANONICAL_PREFIX.strip() or "/texts"
    if not prefix.startswith("/"):
        prefix = f"/{prefix}"
    prefix = prefix.rstrip("/")
    return f"{prefix}/user_{user_id}/file_{file_id}/v{text_version}/normalized.txt"


async def download_file(session: aiohttp.ClientSession, path: str) -> bytes:
    url = build_seaweed_url(path)
    async with session.get(url) as response:
        if response.status != 200:
            raise RuntimeError(f"download failed with status={response.status}, url={url}")
        return await response.read()


async def upload_canonical_text(session: aiohttp.ClientSession, path: str, text: str) -> None:
    url = build_seaweed_url(path)
    form = aiohttp.FormData()
    form.add_field(
        "file",
        text.encode("utf-8"),
        filename="normalized.txt",
        content_type="text/plain; charset=utf-8",
    )

    async with session.post(url, data=form) as response:
        if response.status in (200, 201):
            return
        body = await response.text()
        raise RuntimeError(f"canonical text upload failed status={response.status}, url={url}, body={body[:300]}")


def detect_format(s3_path: str, mime_type: str) -> str:
    extension = PurePosixPath(s3_path).suffix.lower()
    normalized_mime = (mime_type or "").split(";")[0].strip().lower()

    if extension == ".pdf":
        return "pdf"
    if extension == ".docx":
        return "docx"
    if extension == ".txt":
        return "txt"

    if normalized_mime == "application/pdf":
        return "pdf"
    if normalized_mime in (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/docx",
        "application/msword",
    ):
        return "docx"
    if normalized_mime.startswith("text/plain"):
        return "txt"

    raise ValueError(f"unsupported format: ext={extension or '-'} mime={normalized_mime or '-'}")


def normalize_text(text: str) -> str:
    lines = [line.strip() for line in text.splitlines()]
    return "\n".join(line for line in lines if line).strip()


def decode_bytes_with_fallback(data: bytes) -> str:
    for encoding in TEXT_ENCODINGS:
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def coerce_to_text(value: object) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        return decode_bytes_with_fallback(value)
    return str(value)


def maybe_decode_bytes_literal(text: str) -> str:
    candidate = text.strip()
    if not BYTES_LITERAL_RE.match(candidate):
        return text

    try:
        parsed = ast.literal_eval(candidate)
    except (SyntaxError, ValueError):
        return text

    if isinstance(parsed, (bytes, bytearray)):
        return decode_bytes_with_fallback(bytes(parsed))
    return text


def decode_hex_escape_runs(text: str) -> str:
    if "\\x" not in text:
        return text

    def _replace(match: re.Match[str]) -> str:
        chunk = match.group(0)
        hex_payload = chunk.replace("\\x", "")
        try:
            as_bytes = bytes.fromhex(hex_payload)
        except ValueError:
            return chunk
        return decode_bytes_with_fallback(as_bytes)

    return HEX_ESCAPE_RUN_RE.sub(_replace, text)


def sanitize_error_message(err: Exception) -> str:
    message = coerce_to_text(err)
    message = maybe_decode_bytes_literal(message)
    message = message.replace("\n", " ").strip()
    if not message:
        message = "unknown parser error"

    full = f"{err.__class__.__name__}: {message}"
    if len(full) > MAX_ERROR_MESSAGE_LEN:
        return f"{full[:MAX_ERROR_MESSAGE_LEN]} ... [truncated]"
    return full


def extract_from_pdf_bytes(data: bytes) -> str:
    text_chunks: list[str] = []
    reader = pypdf.PdfReader(io.BytesIO(data))
    for page in reader.pages:
        page_text = coerce_to_text(page.extract_text() or "")
        if page_text:
            text_chunks.append(page_text)
    return "\n".join(text_chunks)


def extract_from_docx_bytes(data: bytes) -> str:
    document = docx.Document(io.BytesIO(data))
    return "\n".join(coerce_to_text(paragraph.text) for paragraph in document.paragraphs)


def extract_from_txt_bytes(data: bytes) -> str:
    return decode_bytes_with_fallback(data)


def extract_text(data: bytes, s3_path: str, mime_type: str) -> str:
    file_format = detect_format(s3_path, mime_type)

    if file_format == "pdf":
        text = extract_from_pdf_bytes(data)
    elif file_format == "docx":
        text = extract_from_docx_bytes(data)
    elif file_format == "txt":
        text = extract_from_txt_bytes(data)
    else:
        raise ValueError(f"unsupported format: {file_format}")

    normalized = coerce_to_text(text)
    normalized = maybe_decode_bytes_literal(normalized)
    normalized = decode_hex_escape_runs(normalized)
    return normalize_text(normalized)


async def persist_error(db: asyncpg.Connection, file_id: int, message: str) -> None:
    await db.execute(
        """
        UPDATE files
        SET status = 'ERROR', failure_cause = $1, error_msg = $1
        WHERE id = $2 AND status <> 'READY'
        """,
        message,
        file_id,
    )


async def persist_text_artifact_and_outbox(
    db: asyncpg.Connection,
    session: aiohttp.ClientSession,
    payload: FileMessage,
    normalized_text: str,
) -> tuple[str, int, str]:
    text_hash = hashlib.sha256(normalized_text.encode("utf-8")).hexdigest()
    created_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

    for attempt in range(1, ARTIFACT_WRITE_MAX_ATTEMPTS + 1):
        try:
            async with db.transaction():
                owner_exists = await db.fetchval(
                    "SELECT 1 FROM files WHERE id = $1 AND user_id = $2 FOR UPDATE",
                    payload.file_id,
                    payload.user_id,
                )
                if owner_exists is None:
                    raise ValueError(f"file_id={payload.file_id} does not belong to user_id={payload.user_id}")

                text_version = int(
                    await db.fetchval(
                        "SELECT COALESCE(MAX(text_version), 0) + 1 FROM text_artifacts WHERE file_id = $1",
                        payload.file_id,
                    )
                )
                s3_text_path = build_canonical_text_path(payload.user_id, payload.file_id, text_version)

                # Keep S3 write before DB inserts to satisfy contract ordering.
                await upload_canonical_text(session, s3_text_path, normalized_text)

                event_id = str(uuid.uuid4())
                await db.execute(
                    """
                    INSERT INTO text_artifacts (
                        file_id, user_id, s3_text_path, parser_version, text_version, hash_sha256, index_status
                    ) VALUES ($1, $2, $3, $4, $5, $6, 'QUEUED')
                    """,
                    payload.file_id,
                    payload.user_id,
                    s3_text_path,
                    PARSER_VERSION,
                    text_version,
                    text_hash,
                )

                outbox_payload = json.dumps(
                    {
                        "event_id": event_id,
                        "file_id": payload.file_id,
                        "user_id": payload.user_id,
                        "s3_text_path": s3_text_path,
                        "text_version": text_version,
                        "parser_version": PARSER_VERSION,
                        "created_at": created_at,
                    }
                )
                await db.execute(
                    """
                    INSERT INTO outbox_events (
                        event_id, aggregate_type, aggregate_id, event_type, payload_json, status, attempts
                    ) VALUES ($1::uuid, $2, $3, $4, $5::jsonb, 'NEW', 0)
                    """,
                    event_id,
                    "file",
                    payload.file_id,
                    "text-to-index",
                    outbox_payload,
                )
                return s3_text_path, text_version, event_id
        except Exception as err:  # noqa: BLE001
            if attempt >= ARTIFACT_WRITE_MAX_ATTEMPTS:
                raise
            delay = 2 ** (attempt - 1)
            logger.warning(
                "failed to persist artifact/outbox, retrying",
                extra={
                    "file_id": payload.file_id,
                    "attempt": attempt,
                    "retry_in_sec": delay,
                    "error": str(err),
                },
            )
            await asyncio.sleep(delay)

    raise RuntimeError("failed to persist artifact/outbox after retries")


async def process_message(
    msg_value: bytes,
    redis_client: redis.Redis,
    db: asyncpg.Connection,
    producer: AIOKafkaProducer,
    session: aiohttp.ClientSession,
) -> None:
    try:
        payload = parse_message(msg_value)
    except Exception as err:  # noqa: BLE001
        logger.exception("invalid kafka payload", extra={"error": str(err)})
        return

    started = time.perf_counter()

    logger.info(
        "processing started",
        extra={
            "file_id": payload.file_id,
            "user_id": payload.user_id,
            "s3_path": payload.s3_path,
            "mime_type": payload.mime_type,
        },
    )

    try:
        data = await download_file(session, payload.s3_path)
        logger.info("download complete", extra={"file_id": payload.file_id, "size_bytes": len(data)})

        if len(data) > MAX_FILE_SIZE_BYTES:
            raise ValueError(f"file too large: {len(data)} bytes > {MAX_FILE_SIZE_BYTES}")

        text = await asyncio.to_thread(extract_text, data, payload.s3_path, payload.mime_type)
        logger.info("extraction complete", extra={"file_id": payload.file_id, "text_length": len(text)})

        if len(text) < MIN_TEXT_LEN:
            raise ValueError(f"text too short: {len(text)} < {MIN_TEXT_LEN}")

        cache_key = f"text:{payload.file_id}"
        await redis_client.set(cache_key, text, ex=TEXT_TTL_SECONDS)

        notify = ClassifyMessage(
            file_id=payload.file_id,
            cache_key=cache_key,
            request_id=str(uuid.uuid4()),
        )
        await producer.send_and_wait(TOPIC_OUT, json.dumps(notify.__dict__).encode("utf-8"))

        s3_text_path, text_version, outbox_event_id = await persist_text_artifact_and_outbox(
            db=db,
            session=session,
            payload=payload,
            normalized_text=text,
        )

        logger.info(
            "processing completed",
            extra={
                "file_id": payload.file_id,
                "cache_key": cache_key,
                "text_version": text_version,
                "s3_text_path": s3_text_path,
                "outbox_event_id": outbox_event_id,
                "duration_sec": round(time.perf_counter() - started, 3),
            },
        )
    except Exception as err:  # noqa: BLE001
        logger.exception("processing failed", extra={"file_id": payload.file_id})
        try:
            safe_error = sanitize_error_message(err)
            await persist_error(db, payload.file_id, safe_error)
        except Exception as db_err:  # noqa: BLE001
            logger.exception(
                "failed to persist parser error",
                extra={"file_id": payload.file_id, "error": str(db_err)},
            )


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

    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    db = await asyncpg.connect(POSTGRES_URL)
    session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120))

    await consumer.start()
    await producer.start()

    logger.info(
        "parser service started",
        extra={
            "topic_in": TOPIC_IN,
            "topic_out": TOPIC_OUT,
            "kafka": KAFKA_BROKER,
            "redis": REDIS_URL,
            "db": POSTGRES_URL,
            "seaweed": SEAWEED_URL,
            "parser_version": PARSER_VERSION,
            "text_canonical_prefix": TEXT_CANONICAL_PREFIX,
        },
    )

    try:
        while not stop_event.is_set():
            batches = await consumer.getmany(timeout_ms=1000, max_records=1)
            if not batches:
                continue

            for _partition, messages in batches.items():
                for message in messages:
                    await process_message(message.value, redis_client, db, producer, session)
                    await consumer.commit()
    finally:
        logger.info("parser service stopping")
        await consumer.stop()
        await producer.stop()
        await redis_client.aclose()
        await session.close()
        await db.close()
        logger.info("parser service stopped")


if __name__ == "__main__":
    asyncio.run(main())
