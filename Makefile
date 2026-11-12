GO ?= go
TEST_DB_URL ?= postgres://localhost:5432/eddy_test?sslmode=disable
# The core plus the optional instrumentation modules.
MODULES := . eddyotel eddyprom

.PHONY: test test-db lint

# Unit tests only; integration tests skip without a database.
test:
	@for m in $(MODULES); do (cd $$m && $(GO) test -race -cover ./...) || exit 1; done

# Unit and integration tests against a local Postgres.
test-db:
	@for m in $(MODULES); do (cd $$m && EDDY_TEST_DATABASE_URL=$(TEST_DB_URL) $(GO) test -race -cover ./...) || exit 1; done

lint:
	@for m in $(MODULES); do (cd $$m && $(GO) vet ./...) || exit 1; done
