import asyncio
import base64
import math
import json
import logging
import os
import re
import signal
import time
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone

import aiohttp
import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from fastembed import TextEmbedding
from qdrant_client import QdrantClient
from qdrant_client.models import FieldCondition, Filter, MatchValue

TOPIC_IN = os.getenv("KAFKA_ANALYSIS_REQUESTED_TOPIC", "analysis-requested")
TOPIC_DLQ = os.getenv("KAFKA_ANALYSIS_DLQ_TOPIC", "analysis-dlq")
TOPIC_CHAT_IN = os.getenv("KAFKA_CHAT_REQUESTED_TOPIC", "chat-requested")
TOPIC_CHAT_DLQ = os.getenv("KAFKA_CHAT_DLQ_TOPIC", "chat-dlq")
KAFKA_BROKER = os.getenv("KAFKA_BROKERS", os.getenv("KAFKA_BROKER", "kafka:9092"))
POSTGRES_URL = os.getenv(
    "DB_CONN_STR",
    os.getenv("POSTGRES_URL", "postgresql://myappuser:mypassword@postgres:5432/myapp"),
)
SEAWEED_URL = os.getenv("FILER_URL", os.getenv("SEAWEED_URL", "http://seaweedfs-filer:8888"))
CONSUMER_GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP", "llm_analysis_group")
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "6"))
LLM_TIMEOUT_SECONDS = int(os.getenv("LLM_TIMEOUT_SECONDS", "120"))
KAFKA_STARTUP_RETRY_SECONDS = int(os.getenv("KAFKA_STARTUP_RETRY_SECONDS", "3"))
MODEL_WAIT_INTERVAL_SECONDS = int(os.getenv("MODEL_WAIT_INTERVAL_SECONDS", "5"))
MODEL_WAIT_MAX_SECONDS = int(os.getenv("MODEL_WAIT_MAX_SECONDS", "0"))

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
LLM_MODEL = os.getenv("LLM_MODEL", "haltefest-qwen")
OLLAMA_NUM_CTX = int(os.getenv("OLLAMA_NUM_CTX", "4096"))
LLM_MODEL_FALLBACKS = [
    model.strip()
    for model in os.getenv("LLM_MODEL_FALLBACKS", "qwen2.5:1.5b").split(",")
    if model.strip()
]
LLM_USE_FIRST_AVAILABLE = os.getenv("LLM_USE_FIRST_AVAILABLE", "true").strip().lower() in {"1", "true", "yes"}
QDRANT_URL = os.getenv("QDRANT_URL", "http://qdrant:6333")
COLLECTION_NAME = "file_chunks"
GIGACHAT_AUTH_URL = os.getenv("GIGACHAT_AUTH_URL", "https://ngw.devices.sberbank.ru:9443/api/v2/oauth")
GIGACHAT_API_URL = os.getenv("GIGACHAT_API_URL", "https://gigachat.devices.sberbank.ru/api/v1")
GIGACHAT_SCOPE = os.getenv("GIGACHAT_SCOPE", "GIGACHAT_API_PERS")
GIGACHAT_MODEL = os.getenv("GIGACHAT_MODEL", "GigaChat")
GIGACHAT_TIMEOUT_SECONDS = int(os.getenv("GIGACHAT_TIMEOUT_SECONDS", str(LLM_TIMEOUT_SECONDS)))
GIGACHAT_VERIFY_SSL = os.getenv("GIGACHAT_VERIFY_SSL", "false").strip().lower() in {"1", "true", "yes"}
GIGACHAT_AUTH_KEY = os.getenv("GIGACHAT_AUTH_KEY", "").strip()
GIGACHAT_CLIENT_ID = os.getenv("GIGACHAT_CLIENT_ID", "").strip()
GIGACHAT_CLIENT_SECRET = os.getenv("GIGACHAT_CLIENT_SECRET", "").strip()
GIGACHAT_TOKEN_REFRESH_SKEW_SECONDS = int(os.getenv("GIGACHAT_TOKEN_REFRESH_SKEW_SECONDS", "60"))

SCHEMA_VERSION = "1.0"
MODEL_NAME = os.getenv("LLM_MODEL_NAME", LLM_MODEL)
MODEL_VERSION = os.getenv("LLM_MODEL_VERSION", "qwen2.5-0.5b")
PROMPT_VERSION = os.getenv("PROMPT_VERSION", "analysis-v1")

MAX_ERROR_MESSAGE_LEN = 1200
RETRY_BACKOFF_SECONDS = [1, 5, 15, 60, 300, 900]

SUMMARY_WINDOW_CHARS = 3000
FLASHCARD_RETRIEVE_TOP_K = 20
FLASHCARD_MAX_CARDS = 10

DOC_CHAT_MODEL_CONTEXT_WINDOW_TOKENS = int(os.getenv("DOC_CHAT_MODEL_CONTEXT_WINDOW_TOKENS", "32000"))
DOC_CHAT_SINGLE_DOC_THRESHOLD_TOKENS = int(os.getenv("DOC_CHAT_SINGLE_DOC_THRESHOLD_TOKENS", "0"))
DOC_CHAT_RESERVED_SYSTEM_TOKENS = int(os.getenv("DOC_CHAT_RESERVED_SYSTEM_TOKENS", "400"))
DOC_CHAT_RESERVED_HISTORY_TOKENS = int(os.getenv("DOC_CHAT_RESERVED_HISTORY_TOKENS", "0"))
DOC_CHAT_RESERVED_OUTPUT_TOKENS = int(os.getenv("DOC_CHAT_RESERVED_OUTPUT_TOKENS", "700"))
DOC_CHAT_SAFETY_MARGIN_TOKENS = int(os.getenv("DOC_CHAT_SAFETY_MARGIN_TOKENS", "300"))
DOC_CHAT_RAG_PER_FILE_LIMIT = int(os.getenv("DOC_CHAT_RAG_PER_FILE_LIMIT", "10"))
DOC_CHAT_RAG_TOTAL_LIMIT = int(os.getenv("DOC_CHAT_RAG_TOTAL_LIMIT", "40"))
DOC_CHAT_CONTEXT_MAX_CHARS = int(os.getenv("DOC_CHAT_CONTEXT_MAX_CHARS", "18000"))
DOC_CHAT_EMBEDDING_MODEL = os.getenv("DOC_CHAT_EMBEDDING_MODEL", "BAAI/bge-small-en-v1.5")
DOC_CHAT_SEMANTIC_TOP_K = int(os.getenv("DOC_CHAT_SEMANTIC_TOP_K", "10"))
DOC_CHAT_BM25_TOP_K = int(os.getenv("DOC_CHAT_BM25_TOP_K", "10"))
DOC_CHAT_RRF_K = int(os.getenv("DOC_CHAT_RRF_K", "60"))
DOC_CHAT_HYBRID_FUSED_LIMIT = int(os.getenv("DOC_CHAT_HYBRID_FUSED_LIMIT", "40"))
DOC_CHAT_BM25_PAGE_SIZE = int(os.getenv("DOC_CHAT_BM25_PAGE_SIZE", "256"))
DOC_CHAT_BM25_MAX_CHUNKS = int(os.getenv("DOC_CHAT_BM25_MAX_CHUNKS", "3000"))
DOC_CHAT_RERANK_TOP_K = int(os.getenv("DOC_CHAT_RERANK_TOP_K", "20"))
RERANKER_URL = os.getenv("RERANKER_URL", "http://reranker:8090")
RERANKER_TIMEOUT_SECONDS = int(os.getenv("RERANKER_TIMEOUT_SECONDS", "60"))

ROUTING_MODE_FULL_CONTEXT = "FULL_CONTEXT"
ROUTING_MODE_RAG = "RAG"
ROUTING_MODE_AUTO = "AUTO"
SCOPE_SINGLE_DOC = "single-doc"
SCOPE_MULTI_DOC = "multi-doc"
SCOPE_ALL_DOCS = "all-docs"
BM25_TOKEN_PATTERN = re.compile(r"[0-9A-Za-zА-Яа-яЁё]+", flags=re.UNICODE)

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("LLMAnalysisService")
ACTIVE_LLM_MODEL = LLM_MODEL
GIGACHAT_ACCESS_TOKEN = ""
GIGACHAT_ACCESS_TOKEN_EXPIRES_AT = 0
DOC_CHAT_EMBEDDER: TextEmbedding | None = None


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


def get_active_model() -> str:
    return ACTIVE_LLM_MODEL


def set_active_model(model_name: str) -> None:
    global ACTIVE_LLM_MODEL
    ACTIVE_LLM_MODEL = model_name


async def fetch_ollama_model_names(session: aiohttp.ClientSession) -> list[str]:
    tags_url = f"{OLLAMA_URL.rstrip('/')}/api/tags"
    async with session.get(tags_url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
        if resp.status != 200:
            body = await resp.text()
            raise RuntimeError(f"Failed to fetch Ollama tags: status={resp.status}, body={body[:300]}")
        data = await resp.json()

    models = data.get("models") or []
    names: list[str] = []
    for item in models:
        if isinstance(item, dict):
            name = item.get("name")
            if isinstance(name, str) and name.strip():
                names.append(name.strip())
    return names


async def resolve_ollama_model(session: aiohttp.ClientSession, preferred_model: str) -> str:
    available = await fetch_ollama_model_names(session)
    candidates = [preferred_model, *[m for m in LLM_MODEL_FALLBACKS if m != preferred_model]]

    for candidate in candidates:
        if candidate in available:
            return candidate

    if LLM_USE_FIRST_AVAILABLE and available:
        return available[0]

    available_text = ", ".join(available) if available else "none"
    raise TerminalError(
        f"Ollama model '{preferred_model}' not found. Available models: {available_text}. "
        f"Set LLM_MODEL/LLM_MODEL_FALLBACKS or pull model via ollama-init."
    )


async def wait_for_ollama_model(session: aiohttp.ClientSession, stop_event: asyncio.Event) -> str:
    started_at = time.monotonic()
    while not stop_event.is_set():
        try:
            return await resolve_ollama_model(session, LLM_MODEL)
        except TerminalError as err:
            elapsed = int(time.monotonic() - started_at)
            logger.warning(
                "ollama model is not ready yet",
                extra={
                    "requested_model": LLM_MODEL,
                    "fallbacks": ",".join(LLM_MODEL_FALLBACKS),
                    "elapsed_sec": elapsed,
                    "retry_in_sec": MODEL_WAIT_INTERVAL_SECONDS,
                    "error": str(err),
                },
            )
            if MODEL_WAIT_MAX_SECONDS > 0 and elapsed >= MODEL_WAIT_MAX_SECONDS:
                raise TerminalError(
                    f"Timed out waiting for Ollama model after {MODEL_WAIT_MAX_SECONDS}s. Last error: {err}"
                ) from err
        except Exception as err:  # noqa: BLE001
            elapsed = int(time.monotonic() - started_at)
            logger.warning(
                "failed to query ollama model tags, retrying",
                extra={
                    "elapsed_sec": elapsed,
                    "retry_in_sec": MODEL_WAIT_INTERVAL_SECONDS,
                    "error": str(err),
                },
            )
            if MODEL_WAIT_MAX_SECONDS > 0 and elapsed >= MODEL_WAIT_MAX_SECONDS:
                raise RuntimeError(
                    f"Timed out waiting for Ollama tags after {MODEL_WAIT_MAX_SECONDS}s: {err}"
                ) from err

        await asyncio.sleep(MODEL_WAIT_INTERVAL_SECONDS)

    raise RuntimeError("shutdown requested while waiting for Ollama model")


@dataclass
class AnalysisEvent:
    event_id: str
    job_id: int
    file_id: int
    user_id: int
    analysis_type: str
    provider: str
    requested_at: str

    def to_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "job_id": self.job_id,
            "file_id": self.file_id,
            "user_id": self.user_id,
            "analysis_type": self.analysis_type,
            "provider": self.provider,
            "requested_at": self.requested_at,
        }


@dataclass
class ChatEvent:
    event_id: str
    job_id: int
    chat_id: int
    question_message_id: int
    user_id: int
    provider: str
    scope_mode: str
    selected_file_ids: list[int]
    requested_at: str

    def to_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "job_id": self.job_id,
            "chat_id": self.chat_id,
            "question_message_id": self.question_message_id,
            "user_id": self.user_id,
            "provider": self.provider,
            "scope_mode": self.scope_mode,
            "selected_file_ids": self.selected_file_ids,
            "requested_at": self.requested_at,
        }


def parse_event(raw_value: bytes) -> AnalysisEvent:
    payload = json.loads(raw_value.decode("utf-8"))
    analysis_type = str(payload["analysis_type"]).strip().lower()
    if analysis_type not in {"summary", "chapters", "flashcards"}:
        raise ValueError("invalid analysis_type")
    provider = str(payload.get("provider", "local")).strip().lower()
    if provider not in {"local", "gigachat"}:
        raise ValueError("invalid provider")
    return AnalysisEvent(
        event_id=str(payload["event_id"]),
        job_id=int(payload["job_id"]),
        file_id=int(payload["file_id"]),
        user_id=int(payload["user_id"]),
        analysis_type=analysis_type,
        provider=provider,
        requested_at=str(payload["requested_at"]),
    )


def parse_chat_event(raw_value: bytes) -> ChatEvent:
    payload = json.loads(raw_value.decode("utf-8"))
    provider = str(payload["provider"]).strip().lower()
    if provider not in {"local", "gigachat"}:
        raise ValueError("invalid provider")

    scope_mode = str(payload["scope_mode"]).strip().lower()
    if scope_mode not in {SCOPE_SINGLE_DOC, SCOPE_MULTI_DOC, SCOPE_ALL_DOCS}:
        raise ValueError("invalid scope_mode")

    selected_raw = payload.get("selected_file_ids", [])
    if not isinstance(selected_raw, list):
        raise ValueError("selected_file_ids must be an array")

    selected_ids: list[int] = []
    seen_ids: set[int] = set()
    for value in selected_raw:
        file_id = int(value)
        if file_id <= 0:
            continue
        if file_id in seen_ids:
            continue
        seen_ids.add(file_id)
        selected_ids.append(file_id)

    if scope_mode == SCOPE_SINGLE_DOC and len(selected_ids) != 1:
        raise ValueError("single-doc scope requires exactly one selected file")
    if scope_mode == SCOPE_MULTI_DOC and len(selected_ids) < 2:
        raise ValueError("multi-doc scope requires at least two selected files")
    if scope_mode == SCOPE_ALL_DOCS and len(selected_ids) != 0:
        raise ValueError("all-docs scope does not accept selected_file_ids")

    return ChatEvent(
        event_id=str(payload["event_id"]),
        job_id=int(payload["job_id"]),
        chat_id=int(payload["chat_id"]),
        question_message_id=int(payload["question_message_id"]),
        user_id=int(payload["user_id"]),
        provider=provider,
        scope_mode=scope_mode,
        selected_file_ids=selected_ids,
        requested_at=str(payload["requested_at"]),
    )


def now_rfc3339() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def sanitize_error_message(err: Exception) -> str:
    message = f"{err.__class__.__name__}: {err}".replace("\n", " ").strip()
    if not message:
        message = "unknown llm analysis error"
    if len(message) <= MAX_ERROR_MESSAGE_LEN:
        return message
    return f"{message[:MAX_ERROR_MESSAGE_LEN]} ... [truncated]"


def get_retry_delay(attempt: int) -> int:
    idx = max(0, min(attempt - 1, len(RETRY_BACKOFF_SECONDS) - 1))
    return RETRY_BACKOFF_SECONDS[idx]


def normalize_text(text: str) -> str:
    lines = [line.strip() for line in text.splitlines()]
    return "\n".join([line for line in lines if line]).strip()


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def trim_wrapping_quotes(text: str) -> str:
    value = text.strip()
    if len(value) >= 2 and ((value[0] == '"' and value[-1] == '"') or (value[0] == "'" and value[-1] == "'")):
        return value[1:-1].strip()
    return value


def extract_json_block(text: str) -> str:
    stripped = text.strip()
    fenced = re.search(r"```(?:json)?\s*(.*?)```", stripped, flags=re.IGNORECASE | re.DOTALL)
    if fenced:
        return fenced.group(1).strip()

    start = stripped.find("{")
    end = stripped.rfind("}")
    if start >= 0 and end > start:
        return stripped[start : end + 1].strip()

    return stripped


def remove_label_prefix(text: str, labels: tuple[str, ...]) -> str:
    value = text.strip()
    pattern = r"^\s*(?:[-*]|\d+[.)])?\s*(?:" + "|".join(re.escape(label) for label in labels) + r")\s*:\s*"
    while True:
        updated = re.sub(pattern, "", value, flags=re.IGNORECASE).strip()
        if updated == value:
            break
        value = updated
    return value


def trim_at_next_question_label(text: str) -> str:
    match = re.search(r"(?:^|\n)\s*(?:вопрос|question)\s*:", text, flags=re.IGNORECASE)
    if match:
        return text[: match.start()].strip()
    return text.strip()


def parse_flashcard_from_json(raw: str) -> tuple[str, str] | None:
    payload = extract_json_block(raw)
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return None

    if not isinstance(data, dict):
        return None

    question = data.get("question")
    answer = data.get("answer")

    if question is None:
        question = data.get("вопрос")
    if answer is None:
        answer = data.get("ответ")

    if isinstance(question, str) and isinstance(answer, str):
        return question, answer
    return None


def parse_flashcard_from_labels(raw: str) -> tuple[str, str] | None:
    text = raw.strip()
    question_match = re.search(r"(?:вопрос|question)\s*:", text, flags=re.IGNORECASE)
    answer_match = re.search(r"(?:ответ|answer)\s*:", text, flags=re.IGNORECASE)
    if not question_match or not answer_match or answer_match.start() <= question_match.start():
        return None

    question = text[question_match.end() : answer_match.start()].strip()
    answer = text[answer_match.end() :].strip()
    return question, answer


def parse_flashcard_fallback(raw: str) -> tuple[str, str] | None:
    lines = [normalize_whitespace(line) for line in raw.splitlines() if normalize_whitespace(line)]
    if len(lines) < 2:
        return None
    return lines[0], " ".join(lines[1:])


def normalize_flashcard(question: str, answer: str) -> tuple[str, str] | None:
    q = trim_wrapping_quotes(normalize_whitespace(question))
    a = trim_wrapping_quotes(normalize_whitespace(answer))

    q = remove_label_prefix(q, ("Вопрос", "Question"))
    a = remove_label_prefix(a, ("Ответ", "Answer"))
    a = trim_at_next_question_label(a)

    q = normalize_whitespace(q)
    a = normalize_whitespace(a)

    if not q or not a:
        return None

    if len(q) < 8 or len(a) < 12:
        return None

    if q.lower() == a.lower():
        return None

    if not q.endswith("?"):
        q = f"{q}?"

    return q, a


def parse_and_normalize_flashcard(raw: str) -> tuple[str, str] | None:
    parsed = parse_flashcard_from_json(raw)
    if parsed is None:
        parsed = parse_flashcard_from_labels(raw)
    if parsed is None:
        parsed = parse_flashcard_fallback(raw)
    if parsed is None:
        return None
    question, answer = parsed
    return normalize_flashcard(question, answer)


def flashcard_key(text: str) -> str:
    normalized = normalize_whitespace(text).lower()
    return re.sub(r"[^a-zа-яё0-9]+", " ", normalized, flags=re.IGNORECASE).strip()


def split_into_windows(text: str, window_chars: int) -> list[str]:
    if len(text) <= window_chars:
        return [text]

    windows: list[str] = []
    start = 0
    while start < len(text):
        end = start + window_chars
        if end >= len(text):
            windows.append(text[start:])
            break

        boundary = text.rfind("\n", start, end)
        if boundary <= start:
            boundary = text.rfind(". ", start, end)
            if boundary > start:
                boundary += 1

        if boundary <= start:
            boundary = end
        else:
            boundary += 1

        windows.append(text[start:boundary].strip())
        start = boundary

    return [w for w in windows if w]


async def call_ollama(session: aiohttp.ClientSession, prompt: str, timeout: int | None = None) -> str:
    effective_timeout = timeout or LLM_TIMEOUT_SECONDS
    url = f"{OLLAMA_URL.rstrip('/')}/api/generate"
    model_name = get_active_model()
    payload = {
        "model": model_name,
        "prompt": prompt,
        "stream": False,
        "options": {
            "num_predict": 2048,
            "num_ctx": OLLAMA_NUM_CTX,
        },
    }
    async with session.post(
        url,
        json=payload,
        timeout=aiohttp.ClientTimeout(total=effective_timeout),
    ) as resp:
        if resp.status != 200:
            body = await resp.text()
            body_trimmed = body[:500]
            if resp.status == 404 and "not found" in body.lower():
                resolved_model = await resolve_ollama_model(session, LLM_MODEL)
                if resolved_model != model_name:
                    set_active_model(resolved_model)
                    logger.warning(
                        "switching llm model after 404",
                        extra={"requested_model": model_name, "resolved_model": resolved_model},
                    )
                    return await call_ollama(session, prompt, timeout)
                raise TerminalError(f"Ollama model '{model_name}' not found. body={body_trimmed}")
            raise RuntimeError(f"Ollama API error: status={resp.status}, body={body_trimmed}")
        data = await resp.json()
        return data.get("response", "").strip()


def resolve_gigachat_basic_key() -> str:
    if GIGACHAT_AUTH_KEY:
        key = GIGACHAT_AUTH_KEY.strip()
        if key.lower().startswith("basic "):
            return key[6:].strip()
        return key

    if GIGACHAT_CLIENT_ID and GIGACHAT_CLIENT_SECRET:
        pair = f"{GIGACHAT_CLIENT_ID}:{GIGACHAT_CLIENT_SECRET}".encode("utf-8")
        return base64.b64encode(pair).decode("ascii")

    raise TerminalError(
        "gigachat credentials are not configured; set GIGACHAT_AUTH_KEY or GIGACHAT_CLIENT_ID+GIGACHAT_CLIENT_SECRET"
    )


def gigachat_credentials_configured() -> bool:
    if GIGACHAT_AUTH_KEY:
        return True
    return bool(GIGACHAT_CLIENT_ID and GIGACHAT_CLIENT_SECRET)


def get_gigachat_ssl_option() -> bool:
    return GIGACHAT_VERIFY_SSL


def invalidate_gigachat_token() -> None:
    global GIGACHAT_ACCESS_TOKEN, GIGACHAT_ACCESS_TOKEN_EXPIRES_AT
    GIGACHAT_ACCESS_TOKEN = ""
    GIGACHAT_ACCESS_TOKEN_EXPIRES_AT = 0


def has_valid_gigachat_token() -> bool:
    if not GIGACHAT_ACCESS_TOKEN:
        return False
    now_ts = int(time.time())
    return now_ts + GIGACHAT_TOKEN_REFRESH_SKEW_SECONDS < int(GIGACHAT_ACCESS_TOKEN_EXPIRES_AT)


def parse_gigachat_expiry(value: object) -> int:
    now_ts = int(time.time())
    try:
        expires = int(value)  # unix epoch seconds
    except (TypeError, ValueError):
        expires = now_ts + (30 * 60)

    if expires <= now_ts:
        return now_ts + (25 * 60)
    return expires


async def request_gigachat_token(session: aiohttp.ClientSession) -> tuple[str, int]:
    basic_key = resolve_gigachat_basic_key()
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "RqUID": str(uuid.uuid4()),
        "Authorization": f"Basic {basic_key}",
    }
    data = {"scope": GIGACHAT_SCOPE}

    async with session.post(
        GIGACHAT_AUTH_URL,
        headers=headers,
        data=data,
        timeout=aiohttp.ClientTimeout(total=GIGACHAT_TIMEOUT_SECONDS),
        ssl=get_gigachat_ssl_option(),
    ) as resp:
        body = await resp.text()
        if resp.status in {401, 403}:
            raise TerminalError(f"gigachat auth failed: status={resp.status}, body={body[:300]}")
        if resp.status != 200:
            raise RuntimeError(f"gigachat oauth error: status={resp.status}, body={body[:300]}")

        try:
            payload = json.loads(body)
        except json.JSONDecodeError as err:
            raise RuntimeError(f"gigachat oauth returned invalid json: {err}") from err

    access_token = str(payload.get("access_token") or "").strip()
    if not access_token:
        raise RuntimeError("gigachat oauth response does not contain access_token")
    expires_at = parse_gigachat_expiry(payload.get("expires_at"))
    return access_token, expires_at


async def get_gigachat_token(session: aiohttp.ClientSession) -> str:
    global GIGACHAT_ACCESS_TOKEN, GIGACHAT_ACCESS_TOKEN_EXPIRES_AT
    if has_valid_gigachat_token():
        return GIGACHAT_ACCESS_TOKEN

    token, expires_at = await request_gigachat_token(session)
    GIGACHAT_ACCESS_TOKEN = token
    GIGACHAT_ACCESS_TOKEN_EXPIRES_AT = expires_at
    return token


def extract_gigachat_content(payload: dict) -> str:
    choices = payload.get("choices")
    if not isinstance(choices, list) or len(choices) == 0:
        return ""

    first = choices[0]
    if not isinstance(first, dict):
        return ""
    message = first.get("message")
    if not isinstance(message, dict):
        return ""

    content = message.get("content")
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if not isinstance(item, dict):
                continue
            item_text = item.get("text")
            if isinstance(item_text, str) and item_text.strip():
                parts.append(item_text.strip())
        return "\n".join(parts).strip()
    return ""


async def call_gigachat(session: aiohttp.ClientSession, prompt: str, timeout: int | None = None) -> str:
    effective_timeout = timeout or GIGACHAT_TIMEOUT_SECONDS
    url = f"{GIGACHAT_API_URL.rstrip('/')}/chat/completions"

    async def do_request(access_token: str) -> tuple[int, str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}",
        }
        payload = {
            "model": GIGACHAT_MODEL,
            "messages": [
                {
                    "role": "user",
                    "content": prompt,
                }
            ],
            "temperature": 0.3,
            "stream": False,
        }
        async with session.post(
            url,
            headers=headers,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=effective_timeout),
            ssl=get_gigachat_ssl_option(),
        ) as resp:
            return resp.status, await resp.text()

    token = await get_gigachat_token(session)
    status, body = await do_request(token)
    if status == 401:
        invalidate_gigachat_token()
        token = await get_gigachat_token(session)
        status, body = await do_request(token)

    if status in {401, 403}:
        raise TerminalError(f"gigachat request unauthorized: status={status}, body={body[:300]}")
    if status in {400, 404, 422}:
        raise TerminalError(f"gigachat request rejected: status={status}, body={body[:300]}")
    if status != 200:
        raise RuntimeError(f"gigachat api error: status={status}, body={body[:300]}")

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as err:
        raise RuntimeError(f"gigachat returned invalid json: {err}") from err

    content = extract_gigachat_content(payload)
    if not content:
        raise RuntimeError("gigachat returned empty response content")
    return content


async def call_provider_llm(
    session: aiohttp.ClientSession,
    provider: str,
    prompt: str,
    timeout: int | None = None,
) -> str:
    if provider == "local":
        return await call_ollama(session, prompt, timeout=timeout)
    if provider == "gigachat":
        effective_timeout = timeout if timeout is not None else GIGACHAT_TIMEOUT_SECONDS
        return await call_gigachat(session, prompt, timeout=effective_timeout)
    raise TerminalError(f"unsupported provider '{provider}'")


async def generate_summary(session: aiohttp.ClientSession, text: str, provider: str) -> tuple[str, list[int]]:
    windows = split_into_windows(text, SUMMARY_WINDOW_CHARS)

    if len(windows) == 1:
        prompt = (
            "Ты — помощник для анализа документов. Сделай краткое изложение (summary) следующего текста. "
            "Ответ должен быть на том же языке, что и текст. Пиши связным текстом, без списков и маркеров.\n\n"
            f"Текст:\n{text[:8000]}"
        )
        result = await call_provider_llm(session, provider, prompt)
        return result, [1]

    window_summaries: list[str] = []
    for i, window in enumerate(windows):
        prompt = (
            "Ты — помощник для анализа документов. Сделай краткое изложение (summary) этой части документа. "
            "Пиши связным текстом, без списков. Ответ на том же языке, что и текст.\n\n"
            f"Часть {i+1}/{len(windows)}:\n{window}"
        )
        partial = await call_provider_llm(session, provider, prompt, timeout=60)
        window_summaries.append(partial)

    combined = "\n\n".join(window_summaries)
    final_prompt = (
        "Ты — помощник для анализа документов. Вот краткие изложения частей документа. "
        "Объедини их в одно связное краткое изложение всего документа. "
        "Пиши связным текстом, без списков. Ответ на том же языке, что и текст.\n\n"
        f"Части:\n{combined[:8000]}"
    )
    result = await call_provider_llm(session, provider, final_prompt)
    return result, list(range(1, len(windows) + 1))


def segment_by_headings(text: str) -> list[tuple[str, str]]:
    heading_pattern = re.compile(
        r"^(#{1,3}\s+.+|\d+(\.\d+)*\s+[A-Z\u0410-\u042f\u0401].*|[A-Z\u0410-\u042f\u0401][A-Z\u0410-\u042f\u0401\s]{5,})$",
        re.MULTILINE,
    )

    matches = list(heading_pattern.finditer(text))
    if not matches:
        paragraphs = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
        segments: list[tuple[str, str]] = []
        group: list[str] = []
        for i, para in enumerate(paragraphs):
            group.append(para)
            if len(group) >= 3 or i == len(paragraphs) - 1:
                title = group[0][:80]
                segments.append((title, "\n\n".join(group)))
                group = []
        return segments[:10]

    segments = []
    for i, match in enumerate(matches):
        title = match.group(0).strip()
        start = match.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body = text[start:end].strip()
        segments.append((title, body))

    return segments[:10]


async def generate_chapters(session: aiohttp.ClientSession, text: str, provider: str) -> tuple[str, list[int]]:
    segments = segment_by_headings(text)
    if not segments:
        return "Не удалось выделить главы: документ пуст.", []

    lines: list[str] = []
    source_ids: list[int] = []

    for idx, (title, body) in enumerate(segments, start=1):
        if len(body) > 2000:
            body = body[:2000]
        prompt = (
            "Ты — помощник для анализа документов. Дан раздел документа. "
            "Напиши 1-2 предложения, кратко описывающие содержание этого раздела. "
            "Ответ на том же языке, что и текст. Без списков и маркеров.\n\n"
            f"Заголовок: {title}\n\nТекст раздела:\n{body}"
        )
        summary = await call_provider_llm(session, provider, prompt, timeout=60)
        lines.append(f"Глава {idx}: {title}\n{summary}")
        source_ids.append(idx)

    return "\n\n".join(lines), source_ids


def retrieve_chunks(qdrant: QdrantClient, user_id: int, file_id: int) -> list[dict]:
    query_filter = Filter(
        must=[
            FieldCondition(key="user_id", match=MatchValue(value=user_id)),
            FieldCondition(key="file_id", match=MatchValue(value=file_id)),
        ]
    )

    results = qdrant.scroll(
        collection_name=COLLECTION_NAME,
        scroll_filter=query_filter,
        limit=FLASHCARD_RETRIEVE_TOP_K,
        with_payload=True,
    )

    points = results[0] if results else []
    chunks = []
    for point in points:
        payload = point.payload or {}
        chunks.append({
            "chunk_id": payload.get("chunk_id", 0),
            "chunk_text": payload.get("chunk_text", ""),
        })
    return chunks


def get_doc_chat_embedder() -> TextEmbedding:
    global DOC_CHAT_EMBEDDER
    if DOC_CHAT_EMBEDDER is None:
        logger.info("loading doc-chat embedding model", extra={"embedding_model": DOC_CHAT_EMBEDDING_MODEL})
        DOC_CHAT_EMBEDDER = TextEmbedding(model_name=DOC_CHAT_EMBEDDING_MODEL)
    return DOC_CHAT_EMBEDDER


def embed_query_text(query: str) -> list[float]:
    normalized_query = normalize_whitespace(query)
    if not normalized_query:
        raise TerminalError("question is empty after normalization")

    embedder = get_doc_chat_embedder()
    vectors = list(embedder.embed([normalized_query]))
    if not vectors:
        raise RuntimeError("embedding model returned no vectors")
    return [float(value) for value in vectors[0]]


def parse_chunk_payload(payload: dict, fallback_file_id: int, fallback_text_version: int) -> dict | None:
    chunk_text = str(payload.get("chunk_text", "")).strip()
    if not chunk_text:
        return None

    file_id = int(payload.get("file_id", fallback_file_id) or fallback_file_id)
    chunk_id = int(payload.get("chunk_id", 0) or 0)
    text_version = int(payload.get("text_version", fallback_text_version) or fallback_text_version)
    candidate_id = f"{file_id}:{text_version}:{chunk_id}"
    return {
        "candidate_id": candidate_id,
        "file_id": file_id,
        "chunk_id": chunk_id,
        "text_version": text_version,
        "chunk_text": chunk_text,
    }


def build_qdrant_filter(user_id: int, file_id: int, text_version: int) -> Filter:
    must_conditions = [
        FieldCondition(key="user_id", match=MatchValue(value=user_id)),
        FieldCondition(key="file_id", match=MatchValue(value=file_id)),
    ]
    if text_version > 0:
        must_conditions.append(FieldCondition(key="text_version", match=MatchValue(value=text_version)))
    return Filter(must=must_conditions)


def qdrant_search_points(
    qdrant: QdrantClient,
    query_vector: list[float],
    query_filter: Filter,
    limit: int,
) -> list[object]:
    """Compatibility wrapper for qdrant-client versions with/without `.search()`."""
    if limit <= 0:
        return []

    # qdrant-client <= 1.13
    search_method = getattr(qdrant, "search", None)
    if callable(search_method):
        return search_method(
            collection_name=COLLECTION_NAME,
            query_vector=query_vector,
            query_filter=query_filter,
            limit=limit,
            with_payload=True,
        )

    # qdrant-client >= 1.14
    query_points_method = getattr(qdrant, "query_points", None)
    if not callable(query_points_method):
        raise RuntimeError("Qdrant client does not support search/query_points API")

    try:
        response = query_points_method(
            collection_name=COLLECTION_NAME,
            query=query_vector,
            query_filter=query_filter,
            limit=limit,
            with_payload=True,
        )
    except TypeError:
        # Some versions use `query_filter`, others may use `filter`.
        response = query_points_method(
            collection_name=COLLECTION_NAME,
            query=query_vector,
            filter=query_filter,
            limit=limit,
            with_payload=True,
        )

    points = getattr(response, "points", None)
    if isinstance(points, list):
        return points
    if isinstance(response, list):
        return response
    return []


def semantic_retrieve_chat_chunks(
    qdrant: QdrantClient,
    user_id: int,
    file_ids: list[int],
    text_versions: dict[int, int],
    query_vector: list[float],
    top_k: int,
) -> list[dict]:
    if not file_ids or top_k <= 0:
        return []

    candidates: list[dict] = []
    per_file_limit = max(1, top_k)

    for file_id in file_ids:
        text_version = int(text_versions.get(file_id, 0))
        query_filter = build_qdrant_filter(user_id, file_id, text_version)
        results = qdrant_search_points(
            qdrant=qdrant,
            query_vector=query_vector,
            query_filter=query_filter,
            limit=per_file_limit,
        )
        for point in results:
            payload = point.payload or {}
            parsed = parse_chunk_payload(payload, file_id, text_version)
            if parsed is None:
                continue
            parsed["semantic_score"] = float(getattr(point, "score", 0.0) or 0.0)
            candidates.append(parsed)

    deduped: dict[str, dict] = {}
    for candidate in candidates:
        candidate_id = str(candidate["candidate_id"])
        prev = deduped.get(candidate_id)
        if prev is None or float(candidate.get("semantic_score", 0.0)) > float(prev.get("semantic_score", 0.0)):
            deduped[candidate_id] = candidate

    ranked = sorted(deduped.values(), key=lambda item: float(item.get("semantic_score", 0.0)), reverse=True)
    return ranked[:top_k]


def tokenize_for_bm25(text: str) -> list[str]:
    if not text:
        return []
    return [token.lower() for token in BM25_TOKEN_PATTERN.findall(text)]


def compute_bm25_scores(corpus_tokens: list[list[str]], query_tokens: list[str]) -> list[float]:
    if not corpus_tokens or not query_tokens:
        return [0.0 for _ in corpus_tokens]

    doc_count = len(corpus_tokens)
    doc_lengths = [len(tokens) for tokens in corpus_tokens]
    avg_len = sum(doc_lengths) / doc_count if doc_count > 0 else 1.0

    document_frequency: Counter[str] = Counter()
    for tokens in corpus_tokens:
        for token in set(tokens):
            document_frequency[token] += 1

    k1 = 1.5
    b = 0.75
    scores: list[float] = []

    for tokens, doc_len in zip(corpus_tokens, doc_lengths):
        term_freq = Counter(tokens)
        score = 0.0

        for token in query_tokens:
            freq = term_freq.get(token, 0)
            if freq <= 0:
                continue

            df = document_frequency.get(token, 0)
            idf = math.log(1.0 + (doc_count - df + 0.5) / (df + 0.5))
            norm = k1 * (1.0 - b + b * (doc_len / (avg_len or 1.0)))
            score += idf * ((freq * (k1 + 1.0)) / (freq + norm))

        scores.append(score)

    return scores


def scroll_chunks_for_file(
    qdrant: QdrantClient,
    user_id: int,
    file_id: int,
    text_version: int,
    page_size: int,
    max_chunks: int,
) -> list[dict]:
    if max_chunks <= 0:
        return []

    chunks: list[dict] = []
    offset = None
    query_filter = build_qdrant_filter(user_id, file_id, text_version)
    while len(chunks) < max_chunks:
        batch_limit = min(max(1, page_size), max_chunks - len(chunks))
        points, next_offset = qdrant.scroll(
            collection_name=COLLECTION_NAME,
            scroll_filter=query_filter,
            limit=batch_limit,
            offset=offset,
            with_payload=True,
        )
        if not points:
            break

        for point in points:
            payload = point.payload or {}
            parsed = parse_chunk_payload(payload, file_id, text_version)
            if parsed is None:
                continue
            chunks.append(parsed)
            if len(chunks) >= max_chunks:
                break

        if next_offset is None:
            break
        offset = next_offset

    return chunks


def bm25_retrieve_chat_chunks(
    qdrant: QdrantClient,
    user_id: int,
    file_ids: list[int],
    text_versions: dict[int, int],
    question: str,
    top_k: int,
    page_size: int,
    max_chunks: int,
) -> list[dict]:
    if not file_ids or top_k <= 0 or max_chunks <= 0:
        return []

    query_tokens = tokenize_for_bm25(question)
    if not query_tokens:
        return []

    corpus: list[dict] = []
    per_file_limit = max(1, max_chunks // max(1, len(file_ids)))
    per_file_limit = max(per_file_limit, top_k)
    remaining = max_chunks

    for file_id in file_ids:
        if remaining <= 0:
            break
        text_version = int(text_versions.get(file_id, 0))
        file_limit = min(per_file_limit, remaining)
        chunks = scroll_chunks_for_file(qdrant, user_id, file_id, text_version, page_size, file_limit)
        corpus.extend(chunks)
        remaining = max_chunks - len(corpus)

    if not corpus:
        return []

    deduped: dict[str, dict] = {}
    for chunk in corpus:
        deduped[str(chunk["candidate_id"])] = chunk
    corpus = list(deduped.values())

    corpus_tokens = [tokenize_for_bm25(str(chunk.get("chunk_text", ""))) for chunk in corpus]
    scores = compute_bm25_scores(corpus_tokens, query_tokens)

    ranked: list[dict] = []
    for chunk, score in zip(corpus, scores):
        if score <= 0:
            continue
        item = dict(chunk)
        item["bm25_score"] = float(score)
        ranked.append(item)

    ranked.sort(key=lambda item: float(item.get("bm25_score", 0.0)), reverse=True)
    return ranked[:top_k]


def fuse_hybrid_results(semantic_chunks: list[dict], bm25_chunks: list[dict], rrf_k: int, fused_limit: int) -> list[dict]:
    fused: dict[str, dict] = {}

    def apply(source_chunks: list[dict], rank_field: str, score_field: str) -> None:
        for rank, chunk in enumerate(source_chunks, start=1):
            candidate_id = str(chunk["candidate_id"])
            existing = fused.get(candidate_id)
            if existing is None:
                existing = dict(chunk)
                existing["rrf_score"] = 0.0
                existing["semantic_rank"] = None
                existing["bm25_rank"] = None
                fused[candidate_id] = existing

            existing["rrf_score"] = float(existing.get("rrf_score", 0.0)) + (1.0 / float(max(1, rrf_k) + rank))
            existing[rank_field] = rank
            if score_field in chunk:
                existing[score_field] = float(chunk.get(score_field, 0.0) or 0.0)

    apply(semantic_chunks, "semantic_rank", "semantic_score")
    apply(bm25_chunks, "bm25_rank", "bm25_score")

    ranked = sorted(fused.values(), key=lambda item: float(item.get("rrf_score", 0.0)), reverse=True)
    return ranked[: max(1, fused_limit)]


async def rerank_chunks(
    session: aiohttp.ClientSession,
    question: str,
    chunks: list[dict],
    top_k: int,
) -> list[dict]:
    if not chunks:
        return []

    effective_top_k = max(1, min(top_k, len(chunks)))
    payload = {
        "query": question,
        "top_k": effective_top_k,
        "candidates": [
            {
                "candidate_id": str(chunk["candidate_id"]),
                "text": str(chunk["chunk_text"]),
                "file_id": int(chunk["file_id"]),
                "chunk_id": int(chunk["chunk_id"]),
            }
            for chunk in chunks
        ],
    }

    url = f"{RERANKER_URL.rstrip('/')}/rerank"
    try:
        async with session.post(
            url,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=RERANKER_TIMEOUT_SECONDS),
        ) as resp:
            body = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"reranker returned status={resp.status}, body={body[:300]}")
            response_payload = json.loads(body)
    except Exception as err:  # noqa: BLE001
        logger.warning("reranker unavailable, fallback to RRF result", extra={"error": str(err)})
        return chunks[:effective_top_k]

    results = response_payload.get("results")
    if not isinstance(results, list) or not results:
        return chunks[:effective_top_k]

    source_by_id = {str(chunk["candidate_id"]): chunk for chunk in chunks}
    reranked: list[dict] = []
    for item in results:
        if not isinstance(item, dict):
            continue

        candidate_id = str(item.get("candidate_id", "")).strip()
        source = source_by_id.get(candidate_id)
        if source is None:
            continue

        updated = dict(source)
        updated["rerank_score"] = float(item.get("score", 0.0) or 0.0)
        reranked.append(updated)

    if not reranked:
        return chunks[:effective_top_k]
    return reranked[:effective_top_k]


async def retrieve_chat_chunks_hybrid(
    session: aiohttp.ClientSession,
    qdrant: QdrantClient,
    user_id: int,
    file_ids: list[int],
    text_versions: dict[int, int],
    question: str,
) -> list[dict]:
    query_vector = await asyncio.to_thread(embed_query_text, question)
    semantic_chunks = await asyncio.to_thread(
        semantic_retrieve_chat_chunks,
        qdrant,
        user_id,
        file_ids,
        text_versions,
        query_vector,
        DOC_CHAT_SEMANTIC_TOP_K,
    )
    bm25_chunks = await asyncio.to_thread(
        bm25_retrieve_chat_chunks,
        qdrant,
        user_id,
        file_ids,
        text_versions,
        question,
        DOC_CHAT_BM25_TOP_K,
        DOC_CHAT_BM25_PAGE_SIZE,
        DOC_CHAT_BM25_MAX_CHUNKS,
    )

    fused_chunks = fuse_hybrid_results(
        semantic_chunks=semantic_chunks,
        bm25_chunks=bm25_chunks,
        rrf_k=DOC_CHAT_RRF_K,
        fused_limit=DOC_CHAT_HYBRID_FUSED_LIMIT,
    )
    reranked_chunks = await rerank_chunks(
        session=session,
        question=question,
        chunks=fused_chunks,
        top_k=DOC_CHAT_RERANK_TOP_K,
    )

    logger.info(
        "chat retrieval completed",
        extra={
            "file_count": len(file_ids),
            "semantic_count": len(semantic_chunks),
            "bm25_count": len(bm25_chunks),
            "fused_count": len(fused_chunks),
            "reranked_count": len(reranked_chunks),
        },
    )
    return reranked_chunks


def diversity_sample(chunks: list[dict], max_cards: int) -> list[dict]:
    if len(chunks) <= max_cards:
        return chunks

    step = len(chunks) / max_cards
    selected: list[dict] = []
    for i in range(max_cards):
        idx = int(i * step)
        selected.append(chunks[idx])
    return selected


async def generate_flashcards(
    session: aiohttp.ClientSession,
    qdrant: QdrantClient,
    user_id: int,
    file_id: int,
    provider: str,
) -> tuple[str, list[int]]:
    chunks = retrieve_chunks(qdrant, user_id, file_id)
    if not chunks:
        return "Не удалось сформировать карточки: текст не проиндексирован.", []

    selected = diversity_sample(chunks, min(len(chunks), FLASHCARD_MAX_CARDS * 2))

    cards: list[tuple[str, str]] = []
    source_chunk_ids: list[int] = []
    seen_questions: set[str] = set()
    seen_cards: set[tuple[str, str]] = set()

    for chunk in selected:
        if len(cards) >= FLASHCARD_MAX_CARDS:
            break

        chunk_text = chunk["chunk_text"]
        if not chunk_text or len(chunk_text.strip()) < 20:
            continue

        prompt = (
            "Ты — помощник для создания обучающих карточек. На основе текста создай ровно одну карточку. "
            "Верни ТОЛЬКО JSON объект без markdown и без пояснений.\n"
            'Формат JSON: {"question":"...","answer":"..."}\n'
            "Требования: вопрос проверяет ключевую идею текста; ответ короткий и конкретный; "
            "внутри question/answer не добавляй префиксы 'Вопрос:'/'Ответ:'.\n\n"
            f"Текст:\n{chunk_text[:1500]}"
        )
        raw_card = await call_provider_llm(session, provider, prompt, timeout=45)
        parsed = parse_and_normalize_flashcard(raw_card)
        if parsed is None:
            continue

        question, answer = parsed
        q_key = flashcard_key(question)
        a_key = flashcard_key(answer)
        if not q_key or not a_key:
            continue
        if q_key in seen_questions:
            continue

        pair_key = (q_key, a_key)
        if pair_key in seen_cards:
            continue

        seen_questions.add(q_key)
        seen_cards.add(pair_key)
        cards.append((question, answer))
        source_chunk_ids.append(int(chunk.get("chunk_id") or 0))

    if not cards:
        return "Не удалось сформировать карточки из содержания документа.", []

    formatted = []
    for i, (question, answer) in enumerate(cards, start=1):
        formatted.append(f"{i}. Вопрос: {question}\nОтвет: {answer}")

    return "\n\n".join(formatted), source_chunk_ids


async def run_analysis(
    analysis_type: str,
    provider: str,
    session: aiohttp.ClientSession,
    text: str,
    qdrant: QdrantClient,
    user_id: int,
    file_id: int,
) -> tuple[str, list[int]]:
    if analysis_type == "summary":
        return await generate_summary(session, text, provider)
    if analysis_type == "chapters":
        return await generate_chapters(session, text, provider)
    if analysis_type == "flashcards":
        return await generate_flashcards(session, qdrant, user_id, file_id, provider)
    raise TerminalError(f"unsupported analysis type: {analysis_type}")


def build_seaweed_url(path: str) -> str:
    normalized_path = path if path.startswith("/") else f"/{path}"
    return f"{SEAWEED_URL.rstrip('/')}{normalized_path}"


async def download_text(session: aiohttp.ClientSession, s3_text_path: str) -> str:
    url = build_seaweed_url(s3_text_path)
    async with session.get(url) as response:
        if response.status != 200:
            body = await response.text()
            raise RuntimeError(f"s3 download failed status={response.status}, url={url}, body={body[:300]}")
        raw = await response.text()
        return normalize_text(raw)


async def claim_job_attempt(db: asyncpg.Connection, event: AnalysisEvent) -> tuple[str, int]:
    row = await db.fetchrow(
        """
        SELECT file_id, user_id, analysis_type, provider, status, attempts
        FROM analysis_jobs
        WHERE id = $1
        FOR UPDATE
        """,
        event.job_id,
    )
    if row is None:
        raise TerminalError(f"analysis job not found: job_id={event.job_id}")

    if int(row["file_id"]) != event.file_id or int(row["user_id"]) != event.user_id:
        raise TerminalError(f"event/job mismatch for job_id={event.job_id}")
    if str(row["analysis_type"]).strip().lower() != event.analysis_type:
        raise TerminalError(f"analysis type mismatch for job_id={event.job_id}")
    if str(row["provider"] or "").strip().lower() != event.provider:
        raise TerminalError(f"provider mismatch for analysis job: job_id={event.job_id}")

    status = str(row["status"] or "").upper()
    if status == "DONE":
        return "DONE", int(row["attempts"] or 0)
    if status == "FAILED":
        return "FAILED", int(row["attempts"] or 0)

    next_attempt = int(row["attempts"] or 0) + 1
    await db.execute(
        """
        UPDATE analysis_jobs
        SET status = 'PROCESSING',
            attempts = $2,
            started_at = COALESCE(started_at, NOW()),
            updated_at = NOW()
        WHERE id = $1
        """,
        event.job_id,
        next_attempt,
    )
    return "PROCESSING", next_attempt


async def get_latest_text_path(db: asyncpg.Connection, event: AnalysisEvent) -> str:
    row = await db.fetchrow(
        """
        SELECT s3_text_path
        FROM text_artifacts
        WHERE file_id = $1 AND user_id = $2
        ORDER BY text_version DESC
        LIMIT 1
        """,
        event.file_id,
        event.user_id,
    )
    if row is None:
        raise RuntimeError(f"canonical text is not ready for file_id={event.file_id}")
    return str(row["s3_text_path"])


def estimate_tokens(text: str) -> int:
    if not text:
        return 0
    return max(1, len(text) // 4)


def get_provider_context_window_tokens(provider: str) -> int:
    if provider == "local":
        return max(1024, OLLAMA_NUM_CTX)
    return max(1024, DOC_CHAT_MODEL_CONTEXT_WINDOW_TOKENS)


def compute_available_context_tokens(provider: str) -> int:
    window_tokens = get_provider_context_window_tokens(provider)
    available = (
        window_tokens
        - DOC_CHAT_RESERVED_SYSTEM_TOKENS
        - DOC_CHAT_RESERVED_HISTORY_TOKENS
        - DOC_CHAT_RESERVED_OUTPUT_TOKENS
        - DOC_CHAT_SAFETY_MARGIN_TOKENS
    )
    return max(0, available)


def compute_single_doc_threshold_tokens(provider: str) -> int:
    if DOC_CHAT_SINGLE_DOC_THRESHOLD_TOKENS > 0:
        return DOC_CHAT_SINGLE_DOC_THRESHOLD_TOKENS
    return compute_available_context_tokens(provider)


async def ensure_user_owns_files(db: asyncpg.Connection, user_id: int, file_ids: list[int]) -> None:
    if not file_ids:
        return

    owned = await db.fetchval(
        """
        SELECT COUNT(*)
        FROM files
        WHERE user_id = $1
          AND id = ANY($2::int[])
        """,
        user_id,
        file_ids,
    )
    if int(owned or 0) != len(file_ids):
        raise TerminalError("one or more selected files are not accessible")


async def resolve_chat_scope_file_ids(db: asyncpg.Connection, event: ChatEvent) -> list[int]:
    if event.scope_mode == SCOPE_ALL_DOCS:
        rows = await db.fetch(
            """
            SELECT DISTINCT file_id
            FROM text_artifacts
            WHERE user_id = $1
              AND index_status = 'DONE'
            ORDER BY file_id
            """,
            event.user_id,
        )
        file_ids = [int(row["file_id"]) for row in rows]
        if not file_ids:
            raise RuntimeError("no indexed documents found for all-docs scope")
        return file_ids

    file_ids = sorted(set(int(file_id) for file_id in event.selected_file_ids if int(file_id) > 0))
    if event.scope_mode == SCOPE_SINGLE_DOC and len(file_ids) != 1:
        raise TerminalError("single-doc scope requires exactly one file")
    if event.scope_mode == SCOPE_MULTI_DOC and len(file_ids) < 2:
        raise TerminalError("multi-doc scope requires at least two files")
    if not file_ids:
        raise TerminalError("selected file list is empty")

    await ensure_user_owns_files(db, event.user_id, file_ids)
    return file_ids


async def get_latest_indexed_text_versions(
    db: asyncpg.Connection,
    user_id: int,
    file_ids: list[int],
) -> dict[int, int]:
    if not file_ids:
        return {}

    rows = await db.fetch(
        """
        SELECT DISTINCT ON (file_id) file_id, text_version
        FROM text_artifacts
        WHERE user_id = $1
          AND file_id = ANY($2::int[])
          AND index_status = 'DONE'
        ORDER BY file_id, text_version DESC
        """,
        user_id,
        file_ids,
    )
    versions = {int(row["file_id"]): int(row["text_version"]) for row in rows}

    missing = [file_id for file_id in file_ids if file_id not in versions]
    if missing:
        raise RuntimeError(f"indexed chunks are not ready for files: {missing}")
    return versions


async def get_latest_text_path_for_file(db: asyncpg.Connection, user_id: int, file_id: int) -> str:
    row = await db.fetchrow(
        """
        SELECT s3_text_path
        FROM text_artifacts
        WHERE file_id = $1
          AND user_id = $2
        ORDER BY text_version DESC
        LIMIT 1
        """,
        file_id,
        user_id,
    )
    if row is None:
        raise RuntimeError(f"canonical text is not ready for file_id={file_id}")
    return str(row["s3_text_path"])


async def get_question_content(db: asyncpg.Connection, event: ChatEvent) -> str:
    row = await db.fetchrow(
        """
        SELECT content
        FROM chat_messages
        WHERE id = $1
          AND chat_id = $2
          AND user_id = $3
          AND role = 'user'
        """,
        event.question_message_id,
        event.chat_id,
        event.user_id,
    )
    if row is None:
        raise TerminalError(f"question message not found: message_id={event.question_message_id}")
    content = str(row["content"] or "").strip()
    if not content:
        raise TerminalError("question message is empty")
    return content


async def claim_chat_job_attempt(db: asyncpg.Connection, event: ChatEvent) -> tuple[str, int]:
    row = await db.fetchrow(
        """
        SELECT
            user_id,
            chat_id,
            question_message_id,
            provider,
            scope_mode,
            status,
            attempts
        FROM chat_jobs
        WHERE id = $1
        FOR UPDATE
        """,
        event.job_id,
    )
    if row is None:
        raise TerminalError(f"chat job not found: job_id={event.job_id}")

    if int(row["user_id"]) != event.user_id:
        raise TerminalError(f"user mismatch for chat job: job_id={event.job_id}")
    if int(row["chat_id"]) != event.chat_id:
        raise TerminalError(f"chat mismatch for chat job: job_id={event.job_id}")
    if int(row["question_message_id"]) != event.question_message_id:
        raise TerminalError(f"question message mismatch for chat job: job_id={event.job_id}")

    db_provider = str(row["provider"] or "").strip().lower()
    if db_provider != event.provider:
        raise TerminalError(f"provider mismatch for chat job: job_id={event.job_id}")
    db_scope_mode = str(row["scope_mode"] or "").strip().lower()
    if db_scope_mode != event.scope_mode:
        raise TerminalError(f"scope mismatch for chat job: job_id={event.job_id}")

    status = str(row["status"] or "").upper()
    if status == "DONE":
        return "DONE", int(row["attempts"] or 0)
    if status == "FAILED":
        return "FAILED", int(row["attempts"] or 0)

    next_attempt = int(row["attempts"] or 0) + 1
    await db.execute(
        """
        UPDATE chat_jobs
        SET status = 'PROCESSING',
            attempts = $2,
            started_at = COALESCE(started_at, NOW()),
            updated_at = NOW()
        WHERE id = $1
        """,
        event.job_id,
        next_attempt,
    )
    return "PROCESSING", next_attempt


async def persist_chat_success(
    db: asyncpg.Connection,
    event: ChatEvent,
    assistant_text: str,
    routing_mode: str,
    threshold_tokens: int | None,
) -> None:
    async with db.transaction():
        assistant_message_id = await db.fetchval(
            """
            INSERT INTO chat_messages (chat_id, user_id, role, content, metadata_json, created_at)
            VALUES ($1, $2, 'assistant', $3, '{}'::jsonb, NOW())
            RETURNING id
            """,
            event.chat_id,
            event.user_id,
            assistant_text,
        )

        await db.execute(
            """
            UPDATE chat_jobs
            SET status = 'DONE',
                assistant_message_id = $2,
                routing_mode = $3,
                threshold_tokens = $4,
                error = NULL,
                finished_at = NOW(),
                updated_at = NOW()
            WHERE id = $1
            """,
            event.job_id,
            assistant_message_id,
            routing_mode,
            threshold_tokens,
        )

        await db.execute(
            """
            UPDATE chat_threads
            SET updated_at = NOW()
            WHERE id = $1
            """,
            event.chat_id,
        )


async def persist_chat_failed(
    db: asyncpg.Connection,
    event: ChatEvent,
    error_message: str,
    routing_mode: str,
    threshold_tokens: int | None,
) -> None:
    await db.execute(
        """
        UPDATE chat_jobs
        SET status = 'FAILED',
            routing_mode = $3,
            threshold_tokens = $4,
            error = $2,
            finished_at = NOW(),
            updated_at = NOW()
        WHERE id = $1
        """,
        event.job_id,
        error_message,
        routing_mode,
        threshold_tokens,
    )


async def persist_chat_queued_for_retry(
    db: asyncpg.Connection,
    event: ChatEvent,
    error_message: str,
    routing_mode: str,
    threshold_tokens: int | None,
) -> None:
    await db.execute(
        """
        UPDATE chat_jobs
        SET status = 'QUEUED',
            routing_mode = $3,
            threshold_tokens = $4,
            error = $2,
            updated_at = NOW()
        WHERE id = $1
        """,
        event.job_id,
        error_message,
        routing_mode,
        threshold_tokens,
    )


async def publish_chat_dlq(
    producer: AIOKafkaProducer,
    event: ChatEvent,
    error_code: str,
    error_message: str,
    attempt: int,
) -> None:
    payload = {
        "event_id": event.event_id,
        "source_topic": TOPIC_CHAT_IN,
        "source_payload": event.to_dict(),
        "error_code": error_code,
        "error_message": error_message,
        "attempt": attempt,
        "failed_at": now_rfc3339(),
    }
    await producer.send_and_wait(TOPIC_CHAT_DLQ, json.dumps(payload).encode("utf-8"))


def build_rag_context_text(chunks: list[dict], max_chars: int) -> str:
    if not chunks:
        return ""

    parts: list[str] = []
    current_len = 0
    for chunk in chunks:
        prefix = f"[file:{chunk['file_id']} chunk:{chunk['chunk_id']}]\n"
        body = str(chunk["chunk_text"]).strip()
        if not body:
            continue
        block = f"{prefix}{body}\n"
        if current_len + len(block) > max_chars:
            break
        parts.append(block)
        current_len += len(block)

    return "\n".join(parts).strip()


def build_chat_prompt(question: str, scope_mode: str, routing_mode: str, context_text: str) -> str:
    if routing_mode == ROUTING_MODE_FULL_CONTEXT:
        return (
            "Ты — ассистент по документам. Ответь на вопрос пользователя, опираясь на предоставленный текст документа. "
            "Если в тексте нет точного ответа, честно скажи об этом. "
            "Пиши кратко и по делу, на языке вопроса пользователя.\n\n"
            f"Вопрос:\n{question}\n\n"
            f"Текст документа:\n{context_text}"
        )

    return (
        "Ты — ассистент по документам. Ответь на вопрос пользователя, опираясь только на предоставленные фрагменты. "
        "Если данных недостаточно, явно напиши, что информации недостаточно. "
        "Пиши кратко и по делу, на языке вопроса пользователя.\n\n"
        f"Scope: {scope_mode}\n"
        f"Вопрос:\n{question}\n\n"
        f"Контекст (retrieval):\n{context_text}"
    )


def fit_context_to_model_window(
    question: str,
    scope_mode: str,
    routing_mode: str,
    provider: str,
    context_text: str,
) -> tuple[str, str]:
    context = context_text
    max_prompt_tokens = max(
        512,
        get_provider_context_window_tokens(provider) - DOC_CHAT_RESERVED_OUTPUT_TOKENS - DOC_CHAT_SAFETY_MARGIN_TOKENS,
    )
    prompt = build_chat_prompt(question, scope_mode, routing_mode, context)
    prompt_tokens = estimate_tokens(prompt)

    if prompt_tokens <= max_prompt_tokens:
        return context, prompt

    while prompt_tokens > max_prompt_tokens and len(context) > 2000:
        ratio = max_prompt_tokens / max(1, prompt_tokens)
        target_len = max(2000, int(len(context) * ratio * 0.92))
        context = context[:target_len]
        if "\n" in context:
            context = context.rsplit("\n", 1)[0]
        prompt = build_chat_prompt(question, scope_mode, routing_mode, context)
        prompt_tokens = estimate_tokens(prompt)

    if prompt_tokens > max_prompt_tokens and len(context) > 2000:
        context = context[:2000]
        prompt = build_chat_prompt(question, scope_mode, routing_mode, context)

    if estimate_tokens(prompt) > max_prompt_tokens:
        logger.warning(
            "chat prompt still exceeds estimated model window after context trim",
            extra={
                "provider": provider,
                "routing_mode": routing_mode,
                "estimated_prompt_tokens": estimate_tokens(prompt),
                "max_prompt_tokens": max_prompt_tokens,
            },
        )
    elif context != context_text:
        logger.info(
            "chat context trimmed to fit model window",
            extra={
                "provider": provider,
                "routing_mode": routing_mode,
                "original_chars": len(context_text),
                "trimmed_chars": len(context),
                "estimated_prompt_tokens": estimate_tokens(prompt),
                "max_prompt_tokens": max_prompt_tokens,
            },
        )

    return context, prompt


async def generate_chat_answer(
    session: aiohttp.ClientSession,
    question: str,
    scope_mode: str,
    routing_mode: str,
    provider: str,
    context_text: str,
) -> str:
    _, prompt = fit_context_to_model_window(
        question=question,
        scope_mode=scope_mode,
        routing_mode=routing_mode,
        provider=provider,
        context_text=context_text,
    )

    if provider == "local":
        answer = await call_ollama(session, prompt, timeout=LLM_TIMEOUT_SECONDS)
    elif provider == "gigachat":
        answer = await call_gigachat(session, prompt, timeout=GIGACHAT_TIMEOUT_SECONDS)
    else:
        raise TerminalError(f"unsupported provider '{provider}'")

    normalized = normalize_text(answer)
    if not normalized:
        raise RuntimeError("llm returned empty response")
    return normalized

async def persist_success(
    db: asyncpg.Connection,
    event: AnalysisEvent,
    result_json: dict,
) -> None:
    async with db.transaction():
        await db.execute(
            """
            INSERT INTO analysis_results (
                job_id, file_id, result_json, schema_version, model_name, model_version, prompt_version, token_usage_json
            ) VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7, $8::jsonb)
            ON CONFLICT (job_id) DO NOTHING
            """,
            event.job_id,
            event.file_id,
            json.dumps(result_json),
            SCHEMA_VERSION,
            MODEL_NAME,
            MODEL_VERSION,
            PROMPT_VERSION,
            json.dumps({}),
        )
        await db.execute(
            """
            UPDATE analysis_jobs
            SET status = 'DONE', error = NULL, finished_at = NOW(), updated_at = NOW()
            WHERE id = $1
            """,
            event.job_id,
        )


async def persist_failed(db: asyncpg.Connection, event: AnalysisEvent, error_message: str) -> None:
    await db.execute(
        """
        UPDATE analysis_jobs
        SET status = 'FAILED', error = $2, finished_at = NOW(), updated_at = NOW()
        WHERE id = $1
        """,
        event.job_id,
        error_message,
    )


async def persist_queued_for_retry(db: asyncpg.Connection, event: AnalysisEvent, error_message: str) -> None:
    await db.execute(
        """
        UPDATE analysis_jobs
        SET status = 'QUEUED', error = $2, updated_at = NOW()
        WHERE id = $1
        """,
        event.job_id,
        error_message,
    )


async def publish_dlq(
    producer: AIOKafkaProducer,
    event: AnalysisEvent,
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


async def process_event(
    event: AnalysisEvent,
    db: asyncpg.Connection,
    session: aiohttp.ClientSession,
    producer: AIOKafkaProducer,
    qdrant: QdrantClient,
) -> None:
    for _ in range(MAX_ATTEMPTS):
        try:
            async with db.transaction():
                state, attempt = await claim_job_attempt(db, event)
            if state == "DONE":
                logger.info("job already completed", extra={"job_id": event.job_id, "event_id": event.event_id})
                return
            if state == "FAILED":
                logger.info("job already failed", extra={"job_id": event.job_id, "event_id": event.event_id})
                return

            s3_text_path = await get_latest_text_path(db, event)
            text = await asyncio.wait_for(download_text(session, s3_text_path), timeout=LLM_TIMEOUT_SECONDS)
            if not text:
                raise RuntimeError("canonical text is empty")

            result_text, source_chunk_ids = await run_analysis(
                event.analysis_type, event.provider, session, text, qdrant, event.user_id, event.file_id
            )
            result_json = {
                "schema_version": SCHEMA_VERSION,
                "analysis_type": event.analysis_type,
                "provider": event.provider,
                "result_text": result_text,
                "source_chunk_ids": source_chunk_ids,
            }

            await persist_success(db, event, result_json)
            logger.info(
                "analysis completed",
                extra={
                    "event_id": event.event_id,
                    "job_id": event.job_id,
                    "analysis_type": event.analysis_type,
                    "provider": event.provider,
                    "attempt": attempt,
                },
            )
            return
        except TerminalError as err:
            safe_error = sanitize_error_message(err)
            await persist_failed(db, event, safe_error)
            await publish_dlq(producer, event, "ANALYSIS_TERMINAL_ERROR", safe_error, MAX_ATTEMPTS)
            logger.error(
                "analysis terminal failure",
                extra={"event_id": event.event_id, "job_id": event.job_id, "error": safe_error},
            )
            return
        except Exception as err:  # noqa: BLE001
            safe_error = sanitize_error_message(err)
            attempts = await db.fetchval("SELECT attempts FROM analysis_jobs WHERE id = $1", event.job_id)
            current_attempt = int(attempts or 0)
            if current_attempt >= MAX_ATTEMPTS:
                await persist_failed(db, event, safe_error)
                await publish_dlq(producer, event, "ANALYSIS_MAX_ATTEMPTS", safe_error, current_attempt)
                logger.error(
                    "analysis failed after retries",
                    extra={
                        "event_id": event.event_id,
                        "job_id": event.job_id,
                        "analysis_type": event.analysis_type,
                        "provider": event.provider,
                        "attempt": current_attempt,
                        "error": safe_error,
                    },
                )
                return

            delay = get_retry_delay(current_attempt)
            await persist_queued_for_retry(db, event, safe_error)
            logger.warning(
                f"analysis retry scheduled: {safe_error}",
                extra={
                    "event_id": event.event_id,
                    "job_id": event.job_id,
                    "analysis_type": event.analysis_type,
                    "provider": event.provider,
                    "attempt": current_attempt,
                    "retry_in_sec": delay,
                },
            )
            await asyncio.sleep(delay)


async def process_chat_event(
    event: ChatEvent,
    db: asyncpg.Connection,
    session: aiohttp.ClientSession,
    producer: AIOKafkaProducer,
    qdrant: QdrantClient,
) -> None:
    for _ in range(MAX_ATTEMPTS):
        routing_mode = default_chat_routing_mode()
        threshold_tokens: int | None = None
        try:
            async with db.transaction():
                state, attempt = await claim_chat_job_attempt(db, event)
            if state == "DONE":
                logger.info("chat job already completed", extra={"job_id": event.job_id, "event_id": event.event_id})
                return
            if state == "FAILED":
                logger.info("chat job already failed", extra={"job_id": event.job_id, "event_id": event.event_id})
                return

            question = await get_question_content(db, event)
            file_ids = await resolve_chat_scope_file_ids(db, event)

            context_text = ""
            if event.scope_mode == SCOPE_SINGLE_DOC:
                file_id = file_ids[0]
                s3_text_path = await get_latest_text_path_for_file(db, event.user_id, file_id)
                full_text = await asyncio.wait_for(download_text(session, s3_text_path), timeout=LLM_TIMEOUT_SECONDS)
                doc_tokens = estimate_tokens(full_text)
                threshold_tokens = compute_single_doc_threshold_tokens(event.provider)

                if doc_tokens <= threshold_tokens:
                    routing_mode = ROUTING_MODE_FULL_CONTEXT
                    context_text = full_text
                else:
                    routing_mode = ROUTING_MODE_RAG
            else:
                routing_mode = ROUTING_MODE_RAG

            if routing_mode == ROUTING_MODE_RAG:
                text_versions = await get_latest_indexed_text_versions(
                    db=db,
                    user_id=event.user_id,
                    file_ids=file_ids,
                )
                chunks = await retrieve_chat_chunks_hybrid(
                    session=session,
                    qdrant=qdrant,
                    user_id=event.user_id,
                    file_ids=file_ids,
                    text_versions=text_versions,
                    question=question,
                )
                dynamic_context_max_chars = max(2000, compute_available_context_tokens(event.provider) * 4)
                effective_context_max_chars = min(DOC_CHAT_CONTEXT_MAX_CHARS, dynamic_context_max_chars)
                context_text = build_rag_context_text(chunks, effective_context_max_chars)
                if not context_text:
                    raise RuntimeError("RAG context is empty (no indexed chunks found)")

            answer = await generate_chat_answer(
                session=session,
                question=question,
                scope_mode=event.scope_mode,
                routing_mode=routing_mode,
                provider=event.provider,
                context_text=context_text,
            )
            await persist_chat_success(db, event, answer, routing_mode, threshold_tokens)

            logger.info(
                "chat job completed",
                extra={
                    "event_id": event.event_id,
                    "job_id": event.job_id,
                    "chat_id": event.chat_id,
                    "scope_mode": event.scope_mode,
                    "routing_mode": routing_mode,
                    "provider": event.provider,
                    "attempt": attempt,
                },
            )
            return
        except TerminalError as err:
            safe_error = sanitize_error_message(err)
            await persist_chat_failed(db, event, safe_error, routing_mode, threshold_tokens)
            await publish_chat_dlq(producer, event, "CHAT_TERMINAL_ERROR", safe_error, MAX_ATTEMPTS)
            logger.error(
                "chat terminal failure",
                extra={"event_id": event.event_id, "job_id": event.job_id, "error": safe_error},
            )
            return
        except Exception as err:  # noqa: BLE001
            safe_error = sanitize_error_message(err)
            attempts = await db.fetchval("SELECT attempts FROM chat_jobs WHERE id = $1", event.job_id)
            current_attempt = int(attempts or 0)

            if current_attempt >= MAX_ATTEMPTS:
                await persist_chat_failed(db, event, safe_error, routing_mode, threshold_tokens)
                await publish_chat_dlq(producer, event, "CHAT_MAX_ATTEMPTS", safe_error, current_attempt)
                logger.error(
                    "chat failed after retries",
                    extra={
                        "event_id": event.event_id,
                        "job_id": event.job_id,
                        "chat_id": event.chat_id,
                        "attempt": current_attempt,
                        "error": safe_error,
                    },
                )
                return

            delay = get_retry_delay(current_attempt)
            await persist_chat_queued_for_retry(db, event, safe_error, routing_mode, threshold_tokens)
            logger.warning(
                f"chat retry scheduled: {safe_error}",
                extra={
                    "event_id": event.event_id,
                    "job_id": event.job_id,
                    "chat_id": event.chat_id,
                    "attempt": current_attempt,
                    "retry_in_sec": delay,
                    "error": safe_error,
                },
            )
            await asyncio.sleep(delay)


def default_chat_routing_mode() -> str:
    return ROUTING_MODE_AUTO


async def main() -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_args: stop_event.set())

    consumer: AIOKafkaConsumer | None = None
    producer: AIOKafkaProducer | None = None
    db: asyncpg.Connection | None = None
    session: aiohttp.ClientSession | None = None
    consumer_started = False
    producer_started = False

    try:
        db = await asyncpg.connect(POSTGRES_URL)
        session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=LLM_TIMEOUT_SECONDS + 30))
        qdrant = QdrantClient(url=QDRANT_URL, check_compatibility=False)

        resolved_model = await wait_for_ollama_model(session, stop_event)
        set_active_model(resolved_model)

        consumer = AIOKafkaConsumer(
            TOPIC_IN,
            TOPIC_CHAT_IN,
            bootstrap_servers=KAFKA_BROKER,
            group_id=CONSUMER_GROUP_ID,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)

        consumer_started = await start_with_kafka_retry(consumer.start, "llm-analysis-consumer", stop_event)
        producer_started = await start_with_kafka_retry(producer.start, "llm-analysis-producer", stop_event)
        if not consumer_started or not producer_started:
            return

        logger.info(
            "llm-analysis service started",
            extra={
                "topic_in": TOPIC_IN,
                "topic_dlq": TOPIC_DLQ,
                "topic_chat_in": TOPIC_CHAT_IN,
                "topic_chat_dlq": TOPIC_CHAT_DLQ,
                "kafka": KAFKA_BROKER,
                "db": POSTGRES_URL,
                "seaweed": SEAWEED_URL,
                "ollama": OLLAMA_URL,
                "model": get_active_model(),
                "ollama_num_ctx": OLLAMA_NUM_CTX,
                "gigachat_api": GIGACHAT_API_URL,
                "gigachat_model": GIGACHAT_MODEL,
                "gigachat_verify_ssl": GIGACHAT_VERIFY_SSL,
                "gigachat_credentials_configured": gigachat_credentials_configured(),
                "qdrant": QDRANT_URL,
                "doc_chat_model_context_window_tokens": DOC_CHAT_MODEL_CONTEXT_WINDOW_TOKENS,
                "doc_chat_single_doc_threshold_tokens": DOC_CHAT_SINGLE_DOC_THRESHOLD_TOKENS,
                "doc_chat_reserved_system_tokens": DOC_CHAT_RESERVED_SYSTEM_TOKENS,
                "doc_chat_reserved_history_tokens": DOC_CHAT_RESERVED_HISTORY_TOKENS,
                "doc_chat_reserved_output_tokens": DOC_CHAT_RESERVED_OUTPUT_TOKENS,
                "doc_chat_safety_margin_tokens": DOC_CHAT_SAFETY_MARGIN_TOKENS,
                "doc_chat_context_max_chars": DOC_CHAT_CONTEXT_MAX_CHARS,
                "doc_chat_embedding_model": DOC_CHAT_EMBEDDING_MODEL,
                "doc_chat_semantic_top_k": DOC_CHAT_SEMANTIC_TOP_K,
                "doc_chat_bm25_top_k": DOC_CHAT_BM25_TOP_K,
                "doc_chat_rrf_k": DOC_CHAT_RRF_K,
                "doc_chat_fused_limit": DOC_CHAT_HYBRID_FUSED_LIMIT,
                "doc_chat_rerank_top_k": DOC_CHAT_RERANK_TOP_K,
                "reranker_url": RERANKER_URL,
                "model_name": MODEL_NAME,
                "model_version": MODEL_VERSION,
                "prompt_version": PROMPT_VERSION,
            },
        )

        while not stop_event.is_set():
            batches = await consumer.getmany(timeout_ms=1000, max_records=1)
            if not batches:
                continue

            for _partition, messages in batches.items():
                for message in messages:
                    if message.topic == TOPIC_IN:
                        try:
                            event = parse_event(message.value)
                        except Exception as err:  # noqa: BLE001
                            logger.exception("invalid analysis-requested payload", extra={"error": str(err)})
                            await consumer.commit()
                            continue

                        await process_event(event, db, session, producer, qdrant)
                    elif message.topic == TOPIC_CHAT_IN:
                        try:
                            chat_event = parse_chat_event(message.value)
                        except Exception as err:  # noqa: BLE001
                            logger.exception("invalid chat-requested payload", extra={"error": str(err)})
                            await consumer.commit()
                            continue

                        await process_chat_event(chat_event, db, session, producer, qdrant)
                    else:
                        logger.warning("received message from unexpected topic", extra={"topic": message.topic})
                    await consumer.commit()
    finally:
        logger.info("llm-analysis service stopping")
        if consumer_started and consumer is not None:
            await consumer.stop()
        elif consumer is not None:
            await consumer.stop()
        if producer_started and producer is not None:
            await producer.stop()
        elif producer is not None:
            await producer.stop()
        if session is not None and not session.closed:
            await session.close()
        if db is not None:
            await db.close()
        logger.info("llm-analysis service stopped")


if __name__ == "__main__":
    asyncio.run(main())
