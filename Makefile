# yuovision-worker Makefile

# Build Docker image
build:
	docker build -t yuovision-worker .

# Run with docker script
run:
	./docker-run.sh --build

run-dev:
	./docker-run.sh --dev --build

run-prod:
	./docker-run.sh --prod --build

# Stop container
stop:
	docker stop yuovision-worker || true

# Show running containers
ps:
	docker ps --filter name=yuovision-worker

# View logs
logs:
	docker logs yuovision-worker

# Development commands
dev:
	go run main.go

fmt:
	go fmt ./...

test:
	go test -v ./...

# Clean up
clean:
	docker stop yuovision-worker || true
	docker rm yuovision-worker || true