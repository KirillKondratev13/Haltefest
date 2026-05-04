# Haltefest Kubernetes (Helmfile, local dev)

Этот каталог содержит минимальный перенос `docker-compose` в Kubernetes для локальной разработки.

## Что входит
- Helmfile с четырьмя релизами:
  - `kafka`: отдельный OCI release `oci://registry-1.docker.io/bitnamicharts/kafka` в KRaft mode с provisioning job для initial topics.
  - `seaweedfs`: отдельный upstream Helm release `seaweedfs/seaweedfs` из официального repo `https://seaweedfs.github.io/seaweedfs/helm`.
  - `haltefest-infra`: postgres, dragonfly, qdrant, ollama, ollama-init hook job.
  - `haltefest-app`: backend, parser, ml, indexer, llm-analysis, reranker, ingress.
- Namespace: `haltefest`.
- Secrets: в Kubernetes Secret (`haltefest-shared-secrets`).
- Storage: `emptyDir` по умолчанию (ephemeral, dev-only).

## 1. Подготовка кластера

Если используешь `vcluster`, базовый поток может выглядеть так:

```bash
vcluster use driver docker
vcluster create haltefest-dev
vcluster connect haltefest-dev
kubectl config current-context
```

Если работаешь, как сейчас, через Docker Desktop / локальный `kubectl` context,
достаточно убедиться, что активный context указывает на нужный dev-кластер.

## 2. Сборка локальных образов
Важно: этот скрипт только собирает host images.
Для cluster runtime может дополнительно понадобиться:
- import образов в node image store;
- или push в registry и pull из кластера.

```bash
./deploy/scripts/build-local-images.sh dev
./deploy/scripts/load-local-images-into-nodes.sh dev
```

## 3. Деплой через Helmfile

```bash
./deploy/scripts/helmfile-dev.sh -f deploy/helmfile.yaml apply
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

Отдельно SeaweedFS настраивается в `deploy/values/seaweedfs.yaml`.

## Примечания
- Initial Kafka topics создаются встроенным Bitnami provisioning job.
- SeaweedFS идет отдельным upstream chart и использует `fullnameOverride: seaweedfs`, чтобы сохранить compose-совместимые DNS-имена `seaweedfs-master`, `seaweedfs-volume`, `seaweedfs-filer`.
- `ollama-init` реализован как Helm hook job (`post-install,post-upgrade`).
- Для локального dev с `pullPolicy: Never` перед `helmfile apply` нужно загрузить app-образы в node image store.
- Если в shell раньше были выставлены временные `HELM_*=/tmp/...`, используй wrapper `deploy/scripts/helmfile-dev.sh`, который принудительно фиксирует Helm plugin/data dirs.
- Для прода нужны PVC/StorageClass и отдельная стратегия секретов (например External Secrets).
