.PHONY: run build clean docker-up docker-down

run:
	go run server.go

build:
	go build -o bin/crdt server.go

clean:
	rm -rf bin/

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

install:
	go mod tidy

dev: docker-up
	sleep 5
	go run server.go