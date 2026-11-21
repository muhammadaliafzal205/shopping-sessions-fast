IMAGE ?= shopping-sessions:latest
.PHONY: bin build fixtures check-dev
bin:                       ## native binary for quick local iteration
	g++ -O3 -std=c++17 -pthread -static -o pipeline src/main.cpp
build:                     ## the scorer's image
	docker buildx build --platform linux/amd64 --load -t $(IMAGE) .
fixtures: bin              ## all 36 edge-case fixtures, no Docker needed
	python3 scripts/run_fixtures.py fixtures -- ./pipeline
check-dev: bin             ## needs dev/ (generate --seed 1 --events 1000000 --clicks 250000 --output dev) and dev-answer/
	rm -rf out && mkdir out && DATA_DIR=dev OUTPUT_DIR=out ./pipeline && python3 scripts/compare.py out dev-answer
