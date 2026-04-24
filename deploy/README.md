# Haltefest Kubernetes (Helmfile, local dev)

Этот каталог содержит минимальный перенос `docker-compose` в Kubernetes для локальной разработки.

## Что входит
- Helmfile с двумя релизами:
  - `haltefest-infra`: postgres, kafka, kafka-init hook job, dragonfly, seaweedfs, qdrant, ollama, ollama-init hook job.
  - `haltefest-app`: backend, parser, ml, indexer, llm-analysis, reranker, ingress.
- Namespace: `haltefest`.
- Secrets: в Kubernetes Secret (`haltefest-shared-secrets`).
- Storage: `emptyDir` по умолчанию (ephemeral, dev-only).

## 1. Подготовка vind/vcluster
Пример базового локального потока:

```bash
vcluster use driver docker
vcluster create haltefest-dev
vcluster connect haltefest-dev
kubectl config current-context
```

## 2. Сборка локальных образов
Важно: при docker driver (vind) используются локальные образы хоста, поэтому отдельный registry не нужен.

```bash
./deploy/scripts/build-local-images.sh dev
```

## 3. Деплой через Helmfile

```bash
cd deploy
helmfile apply
```

## 4. Проверка

```bash
kubectl -n haltefest get pods
kubectl -n haltefest get jobs
kubectl -n haltefest get ingress
```

## 5. Настройка ingress host
По умолчанию host: `haltefest.local`.
Измени в `deploy/values/app.yaml` на твой домен.

## 6. Секреты
Отредактируй `deploy/values/infra.yaml`:
- `sharedSecret.postgresUser`
- `sharedSecret.postgresPassword`
- `sharedSecret.postgresDatabase`
- `sharedSecret.gigachatAuthKey`
- `sharedSecret.gigachatClientId`
- `sharedSecret.gigachatClientSecret`

## Примечания
- `kafka-init` и `ollama-init` реализованы как Helm hook jobs (`post-install,post-upgrade`).
- Для прода нужны PVC/StorageClass и отдельная стратегия секретов (например External Secrets).
