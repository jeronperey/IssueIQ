FROM golang:alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /normalizer ./services/normalizer

FROM alpine:latest
COPY --from=builder /normalizer /normalizer
CMD ["/normalizer"]
