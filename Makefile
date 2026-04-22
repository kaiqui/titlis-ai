install:
	poetry install

dev-install:
	poetry install --with dev

test:
	poetry run pytest tests/ -v --tb=short

test-unit:
	poetry run pytest tests/unit/ -v --tb=short

test-coverage:
	poetry run pytest tests/ -v --cov=src --cov-report=term-missing --cov-fail-under=70

lint:
	poetry run black --check src/ tests/
	poetry run flake8 src/ tests/
	poetry run mypy src/ --ignore-missing-imports

format:
	poetry run black src/ tests/

run:
	poetry run uvicorn src.main:app --host 0.0.0.0 --port 8001 --reload

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; \
	find . -name "*.pyc" -delete 2>/dev/null; \
	rm -rf .coverage .pytest_cache; \
	echo "Cleaned"

dev: dev-install test lint
