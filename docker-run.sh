#!/bin/bash

# Docker run script for yuovision-worker
set -e

# Default values
IMAGE_NAME="yuovision-worker"
CONTAINER_NAME="yuovision-worker"
ENV_FILE=""
MODE="dev"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --env)
      ENV_FILE="$2"
      shift 2
      ;;
    --name)
      CONTAINER_NAME="$2"
      shift 2
      ;;
    --prod)
      MODE="prod"
      ENV_FILE=".env.prod"
      shift
      ;;
    --dev)
      MODE="dev"
      ENV_FILE=".env.dev"
      shift
      ;;
    --build)
      echo "Building Docker image..."
      docker build -t $IMAGE_NAME .
      shift
      ;;
    --help)
      echo "Usage: $0 [options]"
      echo "Options:"
      echo "  --env FILE        Specify environment file"
      echo "  --name NAME       Specify container name (default: yuovision-worker)"
      echo "  --prod            Use production mode (.env.prod)"
      echo "  --dev             Use development mode (.env.dev)"
      echo "  --build           Build image before running"
      echo "  --help            Show this help message"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Set default env file if not specified
if [[ -z "$ENV_FILE" ]]; then
  if [[ "$MODE" == "prod" ]]; then
    ENV_FILE=".env.prod"
  else
    ENV_FILE=".env.dev"
  fi
fi

# Stop and remove existing container if it exists
if docker ps -a --format 'table {{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
  echo "Stopping and removing existing container: $CONTAINER_NAME"
  docker stop $CONTAINER_NAME || true
  docker rm $CONTAINER_NAME || true
fi

# Check if env file exists
if [[ ! -f "$ENV_FILE" ]]; then
  echo "Warning: Environment file '$ENV_FILE' not found"
  echo "Running without environment file"
  ENV_ARGS=""
else
  ENV_ARGS="--env-file $ENV_FILE"
fi

# Check if config directory exists for volume mount
if [[ -d "../config" ]]; then
  VOLUME_ARGS="-v $(pwd)/../config:/app/config:ro -v worker-temp:/tmp"
else
  echo "Warning: ../config directory not found, running without config volume"
  VOLUME_ARGS="-v worker-temp:/tmp"
fi

echo "Starting yuovision-worker container..."
echo "Mode: $MODE"
echo "Container name: $CONTAINER_NAME"
echo "Environment file: $ENV_FILE"

# Create named volume for temporary files if it doesn't exist
docker volume create worker-temp >/dev/null 2>&1 || true

# Run the container
docker run -d \
  --name $CONTAINER_NAME \
  $ENV_ARGS \
  $VOLUME_ARGS \
  --restart unless-stopped \
  $IMAGE_NAME

echo "Container started successfully!"
echo "Worker is processing video files..."
echo ""
echo "To view logs: docker logs $CONTAINER_NAME"
echo "To stop: docker stop $CONTAINER_NAME"
echo "To restart: docker restart $CONTAINER_NAME"