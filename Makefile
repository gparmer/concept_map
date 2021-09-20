BINARY=./target/debug/concept_map

all: test

build: $(wildcard src/*.rs)
	cargo build

$(BINARY): build

test: build
	$(BINARY) < test.csv
