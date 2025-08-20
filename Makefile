# Makefile for dremio-mcp testing

.PHONY: help test test-unit test-e2e-separate clean

# Default target
help:
	@echo "Available targets:"
	@echo "  test              - Run unit tests first, then each e2e test separately"
	@echo "  test-unit         - Run only unit tests (excluding e2e)"
	@echo "  test-e2e          - Run each e2e test file separately"

# Main test target - runs unit tests first, then e2e separately
.PHONY: test test-unit test-e2e
test: test-unit test-e2e
	@echo "All tests completed!"

# Run only unit tests (excluding e2e)
test-unit:
	@echo "Running unit tests ..."
	@uv run pytest tests --ignore=tests/e2e -v -x

# Run each e2e test file separately
test-e2e:
	@echo "Running e2e tests ..."
	@for file in tests/e2e/test_*.py; do \
		if [ -f "$$file" ]; then \
			echo "Running $$file..."; \
			uv run pytest "$$file" -v || exit 1; \
		fi; \
	done
