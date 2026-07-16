# build stage
FROM golang:1.26.1 AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY cmd/ ./cmd
COPY internal/ ./internal
COPY service/ ./service

RUN CGO_ENABLED=0 GOOS=linux go build -o /app/api ./cmd/api

RUN CGO_ENABLED=0 GOOS=linux go build -o /app/worker ./cmd/worker

# run stage
FROM alpine:latest

WORKDIR /app

RUN apk add --no-cache ffmpeg

COPY --from=builder /app/api .
COPY --from=builder /app/worker .
# default if not specified in docker compose
CMD ["./api"]
