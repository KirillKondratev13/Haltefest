# Build stage
FROM golang:1.23-alpine AS builder

# Install build dependencies
RUN apk add --no-cache nodejs npm make git

# Install templ
RUN go install github.com/a-h/templ/cmd/templ@v0.3.977

# Set working directory
WORKDIR /app

# Copy go module file
COPY go.mod ./
RUN go mod download

# Copy frontend config
COPY web/package.json web/package-lock.json ./web/
COPY web/tailwind.config.js ./web/

# Install frontend dependencies
RUN cd web && npm config set fetch-retry-maxtimeout 120000 && npm ci --include=dev

# Copy source code
COPY . .

# Build application and assets explicitly, then verify expected UI classes exist.
# This prevents shipping a minimal CSS bundle when Tailwind content scan misfires.
RUN make build-templ TEMPL=/go/bin/templ \
    && make build-app \
    && npx --prefix web tailwindcss -c web/tailwind.config.js -i web/src/main.css -o web/public/assets/main.css \
    && npx --prefix web esbuild web/src/main.js --bundle --outfile=web/public/assets/main.js --format=esm --external:fs --external:path --loader:.js=jsx \
    && grep -q '\.btn' web/public/assets/main.css \
    && grep -q '\.menu' web/public/assets/main.css \
    && grep -q '\.navbar' web/public/assets/main.css

# Final stage
FROM alpine:latest

WORKDIR /app

# Copy binary
COPY --from=builder /app/bin/myapp .

# Copy assets
COPY --from=builder /app/web/public/assets ./web/public/assets
COPY --from=builder /app/internal/migrations ./internal/migrations

# Expose port
EXPOSE 8081

# Run application
CMD ["./myapp"]
