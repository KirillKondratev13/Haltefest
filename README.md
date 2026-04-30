# Haltefest

Платформа для загрузки файлов и асинхронной обработки текста:
- backend (Go + Templ + HTMX)
- parser worker (Python)
- ML service (Python + NeoBERT)
- indexer service (Python, baseline indexing flow)
- llm-analysis service (Python, baseline analysis flow)
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
tail -f logs/haltefest-indexer/current.log
tail -f logs/haltefest-llm-analysis/current.log
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

### Известные проблемы при сборке с нуля

#### 1. Отсутствует `go.sum`
**Проблема**: Docker build падает с ошибкой "missing go.sum entry".

**Причина**: Файл `.gitignore` исключал все `go.sum` через паттерн `**/go.sum`.

**Решение**:
```bash
# 1. Удалите строку **/go.sum из .gitignore
# 2. Сгенерируйте go.sum (может потребоваться создать временные .go файлы для templ-пакетов)
cd services/backend
# Создайте временные placeholder файлы если нужно
echo "package home" > internal/view/home/home.go
echo "package layout" > internal/view/layout/layout.go
go mod tidy
# Удалите временные файлы
rm internal/view/home/home.go internal/view/layout/layout.go
```

В Dockerfile убедитесь что копируете `go.sum` вместе с `go.mod`:
```dockerfile
COPY go.mod go.sum ./
```

#### 2. Стили не применяются (daisyui)
**Проблема**: Страница загружается без стилей.

**Причина**: `daisyui` v5 требует Tailwind v4, но проект использует Tailwind v3.

**Решение**: Используйте `daisyui` v4 с Tailwind v3:
```json
// services/backend/web/package.json
{
  "devDependencies": {
    "daisyui": "4.12.24",  // Без ^ чтобы избежать обновления до v5
    "tailwindcss": "^3.4.17"
  }
}
```

После изменения переустановите пакеты:
```bash
cd services/backend/web
rm -rf node_modules package-lock.json
npm install
```

#### 3. Dockerfile конфигурация
Если `package-lock.json` отсутствует, используйте `npm install` вместо `npm ci`:
```dockerfile
RUN cd web && npm config set fetch-retry-maxtimeout 120000 && npm install --include=dev
```

## Документация

Рабочая документация проекта ведется локально в `docs/`.
`docs/` не публикуется в GitHub и исключена через `.gitignore`.

## Структура репозитория

- `services/backend` - web backend и frontend-ассеты
- `services/parser` - parser worker
- `services/ml` - ML классификатор
- `services/indexer` - сервис индексации canonical текста
- `services/llm_analysis` - сервис асинхронного анализа (summary/chapters/flashcards)
- `docker-compose.yml` - оркестрация окружения
- `scripts/collect-container-logs.sh` - сбор runtime-логов контейнеров в файлы
- `docs/` - локальная проектная документация (игнорируется git)
