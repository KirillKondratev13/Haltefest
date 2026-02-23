FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN go build -o parser main.go

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/parser .
CMD ["./parser"]
