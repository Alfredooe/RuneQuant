FROM python:3.12-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.7.7 /uv /uvx /bin/

WORKDIR /app

ENV UV_PROJECT_ENVIRONMENT=system

COPY . .

RUN uv sync --locked

CMD ["uv", "run", "main.py"]
