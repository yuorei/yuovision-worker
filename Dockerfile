# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o worker .

# Runtime stage
FROM alpine:latest

# Install FFmpeg for video processing
RUN apk --no-cache add ffmpeg

# Create non-root user
RUN addgroup -g 1001 -S worker && \
    adduser -S worker -u 1001

# Create temp directory with proper permissions
RUN mkdir -p /tmp && chmod 777 /tmp

WORKDIR /app

COPY --from=builder /app/worker .
RUN chown worker:worker /app/worker

USER worker

EXPOSE 8080

CMD ["./worker"]