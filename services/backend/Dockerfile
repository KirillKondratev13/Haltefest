# Build stage
FROM golang:1.23-alpine AS builder

# Install build dependencies
RUN apk add --no-cache nodejs npm make git

# Install templ
RUN go install github.com/a-h/templ/cmd/templ@v0.3.977

# Set working directory
WORKDIR /app

# Copy go module files
COPY go.mod go.sum ./
RUN go mod download

# Copy frontend config
COPY web/package.json web/package-lock.json ./web/
COPY web/tailwind.config.js ./web/

# Install frontend dependencies
RUN cd web && npm config set fetch-retry-maxtimeout 120000 && npm ci

# Copy source code
COPY . .

# Build application (includes templ generation and asset compilation)
# We override TEMPL variable to point to the installed binary in GOPATH
RUN make build TEMPL=/go/bin/templ

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
