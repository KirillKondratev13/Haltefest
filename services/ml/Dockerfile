FROM python:3.10-slim

WORKDIR /app

ARG MODEL_NAME=chandar-lab/NeoBERT
ARG XFORMERS_VERSION=0.0.28.post3
ENV MODEL_NAME=${MODEL_NAME}
ENV HF_HOME=/app/model_cache

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# NeoBERT remote code imports xformers. Installing with --no-deps keeps CPU image small.
RUN pip install --no-cache-dir --no-deps xformers==${XFORMERS_VERSION}

# Cache model artifacts in the image at build time.
RUN python -c "import os; from transformers import AutoTokenizer, AutoModel; model=os.environ['MODEL_NAME']; AutoTokenizer.from_pretrained(model, trust_remote_code=True); AutoModel.from_pretrained(model, trust_remote_code=True)"

COPY . .
CMD ["python", "main.py"]
