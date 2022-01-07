.PHONY: help
help: ## - Show this help message
	@printf "\033[32m\xE2\x9c\x93 usage: make [target]\n\n\033[0m"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: up
up: ## - start docker compose
	@ cd _support/docker && docker-compose -f docker-compose.yml up

.PHONY: build-common
build-common: ## - execute build common tasks clean and mod tidy
	@ go version
	@ go clean
	@ go mod download && go mod tidy
	@ go mod verify

.PHONY: build
build: build-common ## - build a debug binary to the current platform (windows, linux or darwin(mac))
	@ echo building
	@ go build -v ./...
	@ echo "done"

.PHONY: format-check
format-check: ## - check files format using gofmt
	@ ./_support/scripts/ci.sh fmtCheck

.PHONY: format-check
format: ## - apply golang file format using gofmt
	@ ./_support/scripts/ci.sh format

.PHONY: test
test: build-common ## - execute go test command for unit and mocked tests
	@ ./_support/scripts/ci.sh unitTest

.PHONY: integration-test
integration-test: ## - execute go test command for integration tests (aws credentials needed)
	@ go test -v -cover -race ./test

.PHONY: scan
scan: ## - execute static code analysis
	@ ./_support/scripts/ci.sh scan

.PHONY: local-scan
local-scan: ## - execute static code analysis locally
	@ ./_support/scripts/ci.sh localScan

.PHONY: lint
lint: ## - runs golangci-lint
	@ ./_support/scripts/ci.sh lint

.PHONY: lint-docker
lint-docker: ## - runs golangci-lint with docker container
	@ ./_support/scripts/ci.sh lintDocker

.PHONY: sonar-scan
sonar-scan: ## - start sonar qube locally with docker (you will need docker installed in your machine)
	@ # after start, setup a new project with the name sms-local and a new token sms-token, fill the token against the -Dsonar.login= parameter.
	@ # login with user: admin pwd: vmware
	@ $(SHELL) _support/scripts/sonar-scan.sh

.PHONY: sonar-stop
sonar-stop: ## - stop sonar qube docker container
	@ docker stop sonarqube
