import asyncio
import json
import logging
import os
import re
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import aiohttp
import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from qdrant_client import QdrantClient
from qdrant_client.models import FieldCondition, Filter, MatchValue

TOPIC_IN = os.getenv("KAFKA_ANALYSIS_REQUESTED_TOPIC", "analysis-requested")
TOPIC_DLQ = os.getenv("KAFKA_ANALYSIS_DLQ_TOPIC", "analysis-dlq")
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
LLM_MODEL = os.getenv("LLM_MODEL", "qwen2.5:0.5b")
LLM_MODEL_FALLBACKS = [
    model.strip()
    for model in os.getenv("LLM_MODEL_FALLBACKS", "qwen2.5:1.5b").split(",")
    if model.strip()
]
LLM_USE_FIRST_AVAILABLE = os.getenv("LLM_USE_FIRST_AVAILABLE", "true").strip().lower() in {"1", "true", "yes"}
QDRANT_URL = os.getenv("QDRANT_URL", "http://qdrant:6333")
COLLECTION_NAME = "file_chunks"

SCHEMA_VERSION = "1.0"
MODEL_NAME = os.getenv("LLM_MODEL_NAME", LLM_MODEL)
MODEL_VERSION = os.getenv("LLM_MODEL_VERSION", "qwen2.5-0.5b")
PROMPT_VERSION = os.getenv("PROMPT_VERSION", "analysis-v1")

MAX_ERROR_MESSAGE_LEN = 1200
RETRY_BACKOFF_SECONDS = [1, 5, 15, 60, 300, 900]

SUMMARY_WINDOW_CHARS = 3000
FLASHCARD_RETRIEVE_TOP_K = 20
FLASHCARD_MAX_CARDS = 10

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("LLMAnalysisService")
ACTIVE_LLM_MODEL = LLM_MODEL


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
    requested_at: str

    def to_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "job_id": self.job_id,
            "file_id": self.file_id,
            "user_id": self.user_id,
            "analysis_type": self.analysis_type,
            "requested_at": self.requested_at,
        }


def parse_event(raw_value: bytes) -> AnalysisEvent:
    payload = json.loads(raw_value.decode("utf-8"))
    analysis_type = str(payload["analysis_type"]).strip().lower()
    if analysis_type not in {"summary", "chapters", "flashcards"}:
        raise ValueError("invalid analysis_type")
    return AnalysisEvent(
        event_id=str(payload["event_id"]),
        job_id=int(payload["job_id"]),
        file_id=int(payload["file_id"]),
        user_id=int(payload["user_id"]),
        analysis_type=analysis_type,
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
            "temperature": 0.3,
            "num_predict": 2048,
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


async def generate_summary(session: aiohttp.ClientSession, text: str) -> tuple[str, list[int]]:
    windows = split_into_windows(text, SUMMARY_WINDOW_CHARS)

    if len(windows) == 1:
        prompt = (
            "Ты — помощник для анализа документов. Сделай краткое изложение (summary) следующего текста. "
            "Ответ должен быть на том же языке, что и текст. Пиши связным текстом, без списков и маркеров.\n\n"
            f"Текст:\n{text[:8000]}"
        )
        result = await call_ollama(session, prompt)
        return result, [1]

    window_summaries: list[str] = []
    for i, window in enumerate(windows):
        prompt = (
            "Ты — помощник для анализа документов. Сделай краткое изложение (summary) этой части документа. "
            "Пиши связным текстом, без списков. Ответ на том же языке, что и текст.\n\n"
            f"Часть {i+1}/{len(windows)}:\n{window}"
        )
        partial = await call_ollama(session, prompt, timeout=60)
        window_summaries.append(partial)

    combined = "\n\n".join(window_summaries)
    final_prompt = (
        "Ты — помощник для анализа документов. Вот краткие изложения частей документа. "
        "Объедини их в одно связное краткое изложение всего документа. "
        "Пиши связным текстом, без списков. Ответ на том же языке, что и текст.\n\n"
        f"Части:\n{combined[:8000]}"
    )
    result = await call_ollama(session, final_prompt)
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


async def generate_chapters(session: aiohttp.ClientSession, text: str) -> tuple[str, list[int]]:
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
        summary = await call_ollama(session, prompt, timeout=60)
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
        raw_card = await call_ollama(session, prompt, timeout=45)
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
    session: aiohttp.ClientSession,
    text: str,
    qdrant: QdrantClient,
    user_id: int,
    file_id: int,
) -> tuple[str, list[int]]:
    if analysis_type == "summary":
        return await generate_summary(session, text)
    if analysis_type == "chapters":
        return await generate_chapters(session, text)
    if analysis_type == "flashcards":
        return await generate_flashcards(session, qdrant, user_id, file_id)
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
        SELECT file_id, user_id, analysis_type, status, attempts
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
                event.analysis_type, session, text, qdrant, event.user_id, event.file_id
            )
            result_json = {
                "schema_version": SCHEMA_VERSION,
                "analysis_type": event.analysis_type,
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
                        "attempt": current_attempt,
                        "error": safe_error,
                    },
                )
                return

            delay = get_retry_delay(current_attempt)
            await persist_queued_for_retry(db, event, safe_error)
            logger.warning(
                "analysis retry scheduled",
                extra={
                    "event_id": event.event_id,
                    "job_id": event.job_id,
                    "analysis_type": event.analysis_type,
                    "attempt": current_attempt,
                    "retry_in_sec": delay,
                },
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
                "kafka": KAFKA_BROKER,
                "db": POSTGRES_URL,
                "seaweed": SEAWEED_URL,
                "ollama": OLLAMA_URL,
                "model": get_active_model(),
                "qdrant": QDRANT_URL,
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
                    try:
                        event = parse_event(message.value)
                    except Exception as err:  # noqa: BLE001
                        logger.exception("invalid analysis-requested payload", extra={"error": str(err)})
                        await consumer.commit()
                        continue

                    await process_event(event, db, session, producer, qdrant)
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
