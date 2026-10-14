.PHONY: broker server simulator test race bench vet fmt check up

broker:    ## start only the MQTT broker
	docker compose up -d mosquitto

server:    ## hot-reload server (needs `air`)
	air -c .air.server.toml

simulator: ## hot-reload simulator, e.g. make simulator ASSETS=50
	air -c .air.simulator.toml

test:
	go test ./...

race:
	go test -race -count=1 ./...

bench:
	go test -run '^$$' -bench . -benchmem ./internal/state

vet:
	go vet ./...

fmt:
	gofmt -l -w .

check: vet race ## what CI runs

up:        ## everything in containers
	docker compose up --build
