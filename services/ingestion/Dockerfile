FROM golang:alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /ingestion ./services/ingestion

FROM alpine:latest
COPY --from=builder /ingestion /ingestion
CMD ["/ingestion"]
