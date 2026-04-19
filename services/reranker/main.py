import logging
import os
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sentence_transformers import CrossEncoder

MODEL_NAME = os.getenv("RERANKER_MODEL_NAME", "BAAI/bge-reranker-v2-m3")
MODEL_DEVICE = os.getenv("RERANKER_DEVICE", "cpu")
BATCH_SIZE = int(os.getenv("RERANKER_BATCH_SIZE", "16"))
MAX_CANDIDATES = int(os.getenv("RERANKER_MAX_CANDIDATES", "200"))
DEFAULT_TOP_K = int(os.getenv("RERANKER_DEFAULT_TOP_K", "40"))

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("RerankerService")


class CandidateIn(BaseModel):
    candidate_id: str = Field(..., min_length=1)
    text: str = Field(..., min_length=1)
    file_id: Optional[int] = None
    chunk_id: Optional[int] = None


class RerankRequest(BaseModel):
    query: str = Field(..., min_length=1)
    candidates: list[CandidateIn] = Field(default_factory=list)
    top_k: Optional[int] = None


class CandidateOut(BaseModel):
    candidate_id: str
    text: str
    score: float
    file_id: Optional[int] = None
    chunk_id: Optional[int] = None


class RerankResponse(BaseModel):
    model: str
    total_candidates: int
    returned: int
    results: list[CandidateOut]


app = FastAPI(title="haltefest-reranker", version="1.0.0")
_model: Optional[CrossEncoder] = None


def get_model() -> CrossEncoder:
    if _model is None:
        raise RuntimeError("model is not initialized")
    return _model


@app.on_event("startup")
def on_startup() -> None:
    global _model
    logger.info(
        "loading reranker model",
        extra={
            "model": MODEL_NAME,
            "device": MODEL_DEVICE,
        },
    )
    _model = CrossEncoder(
        MODEL_NAME,
        device=MODEL_DEVICE,
        trust_remote_code=True,
    )
    logger.info("reranker model loaded", extra={"model": MODEL_NAME})


@app.get("/health")
def health() -> dict:
    return {
        "status": "ok" if _model is not None else "starting",
        "model": MODEL_NAME,
        "device": MODEL_DEVICE,
        "loaded": _model is not None,
    }


@app.post("/rerank", response_model=RerankResponse)
def rerank(request: RerankRequest) -> RerankResponse:
    query = request.query.strip()
    if not query:
        raise HTTPException(status_code=400, detail="query must not be empty")

    valid_candidates: list[CandidateIn] = []
    for candidate in request.candidates:
        text = candidate.text.strip()
        if not text:
            continue
        valid_candidates.append(
            CandidateIn(
                candidate_id=candidate.candidate_id,
                text=text,
                file_id=candidate.file_id,
                chunk_id=candidate.chunk_id,
            )
        )

    if not valid_candidates:
        return RerankResponse(model=MODEL_NAME, total_candidates=0, returned=0, results=[])

    if len(valid_candidates) > MAX_CANDIDATES:
        valid_candidates = valid_candidates[:MAX_CANDIDATES]

    top_k = request.top_k or DEFAULT_TOP_K
    top_k = max(1, min(top_k, len(valid_candidates)))

    model = get_model()

    pairs = [[query, candidate.text] for candidate in valid_candidates]
    try:
        scores = model.predict(
            pairs,
            batch_size=BATCH_SIZE,
            show_progress_bar=False,
            convert_to_numpy=True,
        )
    except Exception as err:  # noqa: BLE001
        logger.exception("reranking failed", extra={"error": str(err)})
        raise HTTPException(status_code=500, detail=f"reranking failed: {err}") from err

    ranked: list[CandidateOut] = []
    for candidate, score in zip(valid_candidates, scores):
        ranked.append(
            CandidateOut(
                candidate_id=candidate.candidate_id,
                text=candidate.text,
                file_id=candidate.file_id,
                chunk_id=candidate.chunk_id,
                score=float(score),
            )
        )

    ranked.sort(key=lambda item: item.score, reverse=True)
    result = ranked[:top_k]

    return RerankResponse(
        model=MODEL_NAME,
        total_candidates=len(valid_candidates),
        returned=len(result),
        results=result,
    )
