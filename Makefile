BINARY := gokubedump
LDFLAGS := -s -w

.PHONY: build build-linux tidy clean

build:
	go build -ldflags="$(LDFLAGS)" -o $(BINARY) .

build-linux:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags="$(LDFLAGS)" -o $(BINARY)-linux-amd64 .

tidy:
	go mod tidy

clean:
	rm -f $(BINARY) $(BINARY)-linux-amd64
