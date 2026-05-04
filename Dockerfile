FROM python:3.12-slim-bullseye

WORKDIR /app

RUN pip install --no-cache-dir "poetry>=2.1.0,<3.0.0" && \
    poetry config virtualenvs.create false

COPY pyproject.toml poetry.lock* ./
RUN poetry install --only main --no-interaction --no-ansi --no-root

COPY src/ ./src/

EXPOSE 8001

CMD ["ddtrace-run", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8001"]
