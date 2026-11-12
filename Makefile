GO ?= go
TEST_DB_URL ?= postgres://localhost:5432/eddy_test?sslmode=disable

.PHONY: test test-db lint

# Unit tests only; integration tests skip without a database.
test:
	$(GO) test -race -cover ./...

# Unit and integration tests against a local Postgres.
test-db:
	EDDY_TEST_DATABASE_URL=$(TEST_DB_URL) $(GO) test -race -cover ./...

lint:
	$(GO) vet ./...
