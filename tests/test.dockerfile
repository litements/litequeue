FROM debian:trixie-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    python3 \
    && rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh | sh

ENV PATH="/root/.local/bin:$PATH"
ENV UV_NO_MANAGED_PYTHON=1
ENV UV_PROJECT_ENVIRONMENT=/opt/litequeue-venv
ENV UV_PYTHON=/usr/bin/python3

WORKDIR /build
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --group dev

COPY tests/build_sqlite.sh /usr/local/bin/build-sqlite
ARG SQLITE_RELEASES
RUN test -n "$SQLITE_RELEASES" \
    && for sqlite_release in $SQLITE_RELEASES; do \
        sqlite_version="${sqlite_release%%:*}"; \
        release_tail="${sqlite_release#*:}"; \
        sqlite_year="${release_tail%%:*}"; \
        sqlite_archive_version="${release_tail#*:}"; \
        bash /usr/local/bin/build-sqlite \
            "$sqlite_version" \
            "$sqlite_year" \
            "$sqlite_archive_version"; \
    done

ENV PYTHONPATH=/workspace/src
WORKDIR /workspace
CMD ["sleep", "infinity"]
