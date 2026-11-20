.PHONY: build run clean tidy

BINARY=bin/fsrobot

build:
	mkdir -p bin
	go build -trimpath -ldflags="-s -w" -o $(BINARY) ./cmd/bot

run: build
	./$(BINARY)

tidy:
	go mod tidy

clean:
	rm -rf bin sessions
