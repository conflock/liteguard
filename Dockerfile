FROM golang:1.23-alpine AS builder

WORKDIR /build
COPY go.mod go.sum* ./
RUN go mod download 2>/dev/null || true
COPY . .

ARG VERSION=dev
RUN CGO_ENABLED=0 go build -ldflags="-s -w -X main.version=${VERSION}" \
    -o /liteguard ./cmd/liteguard

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /liteguard /usr/local/bin/liteguard

ENTRYPOINT ["liteguard"]
