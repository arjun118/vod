FROM golang:1.26.1 AS builder

WORKDIR /app

COPY . .

COPY go.mod main.go ./

RUN go mod download

COPY internal ./internal
COPY service ./service

RUN CGO_ENABLED=0 GOOS=linux go build -o server .

FROM alpine:latest

WORKDIR /app

RUN apk add --no-cache ffmpeg

COPY --from=builder /app/server .

CMD ["./server"]
