.PHONY: format clean test help

# Default target
.DEFAULT_GOAL := help

# Format Python code using Ruff
format:
	@echo "Formatting Python code with Ruff..."
	ruff format .
	ruff check --fix .
	@echo "Formatting complete."

# Run tests with pytest
test:
	@echo "Running tests with pytest..."
	pytest -v test_stream.py
	@echo "Tests complete."

# Clean Python cache files
clean:
	@echo "Cleaning Python cache files..."
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.pyd" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name "*.egg" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".coverage" -exec rm -rf {} +
	find . -type d -name "htmlcov" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +
	@echo "Cleaning complete."

# Display help information
help:
	@echo "Available targets:"
	@echo "  format  - Format Python code using Ruff"
	@echo "  test    - Run tests with pytest"
	@echo "  clean   - Remove Python cache files and directories"
	@echo "  help    - Display this help message"
