FROM golang:alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /indexer ./services/indexer

FROM alpine:latest
COPY --from=builder /indexer /indexer
CMD ["/indexer"]
