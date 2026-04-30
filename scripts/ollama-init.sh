#!/bin/sh

echo 'Waiting for Ollama to be ready...'
until ollama list > /dev/null 2>&1; do
  sleep 2
done

# Pull the base model
BASE_MODEL=""
echo 'Trying to pull preferred models...'
for model in qwen2.5:0.5b qwen2.5:1.5b; do
  if [ -z "$BASE_MODEL" ]; then
    echo "Pulling $model ..."
    if ollama pull "$model"; then
      echo "Model pulled successfully: $model"
      BASE_MODEL="$model"
    fi
  fi
done

if [ -z "$BASE_MODEL" ]; then
  echo 'Failed to pull any preferred model' >&2
  exit 1
fi

# Create custom model with temperature from environment variables
echo "Creating custom model with temperature=${OLLAMA_TEMPERATURE}..."
cat > /tmp/Modelfile <<EOF
FROM ${BASE_MODEL}
PARAMETER temperature ${OLLAMA_TEMPERATURE}
PARAMETER num_ctx ${OLLAMA_NUM_CTX}
EOF
ollama create haltefest-qwen -f /tmp/Modelfile
echo "Custom model 'haltefest-qwen' created successfully"
