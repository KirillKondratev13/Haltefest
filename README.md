# Haltefest

Платформа для загрузки файлов и асинхронной обработки текста:
- backend (Go + Templ + HTMX)
- parser worker (Python)
- ML service (Python + NeoBERT)
- Kafka, DragonflyDB (Redis), PostgreSQL, SeaweedFS

## Быстрый старт

Требования:
- Docker
- Docker Compose

Запуск всего стека:

```bash
docker compose up -d --build
```

Приложение: `http://localhost:8081`

Миграции БД применяются автоматически на старте backend.

## Логи

После старта `docker compose up -d` runtime-логи контейнеров пишутся в:
- `logs/<container>/current.log`
- `logs/<container>/<timestamp>.log`

Примеры:

```bash
tail -f logs/haltefest-backend/current.log
tail -f logs/haltefest-parser/current.log
tail -f logs/haltefest-ml/current.log
```

## Локальная разработка

Основной код backend находится в `services/backend`.

Полезные команды:

```bash
cd services/backend
make build
make watch
```

Если менялся Go/JS/CSS backend и ты запускаешь через Docker, пересобери backend-образ:

```bash
docker compose up -d --build backend
```

## Документация

Рабочая документация проекта ведется локально в `docs/`.
`docs/` не публикуется в GitHub и исключена через `.gitignore`.

## Структура репозитория

- `services/backend` - web backend и frontend-ассеты
- `services/parser` - parser worker
- `services/ml` - ML классификатор
- `docker-compose.yml` - оркестрация окружения
- `scripts/collect-container-logs.sh` - сбор runtime-логов контейнеров в файлы
- `docs/` - локальная проектная документация (игнорируется git)
