FROM golang:alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /query ./services/query

FROM alpine:latest
COPY --from=builder /query /query
CMD ["/query"]
