# Multi-stage Dockerfile for Endterm Microservices

FROM golang:1.26-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG SERVICE_NAME=gateway
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/service ./cmd/${SERVICE_NAME}

FROM alpine:3.19
RUN apk --no-cache add ca-certificates curl

COPY --from=builder /app/bin/service /usr/local/bin/service
USER 65534:65534

EXPOSE 8080 8081 8082 8083

ENTRYPOINT ["/usr/local/bin/service"]
