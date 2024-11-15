.PHONY: test
test:
	@go test -v --cover $(go list ./... | grep -v /examples/)

.PHONY: test-short
test-short:
	@go test -short -v --cover $(go list ./... | grep -v /examples/)