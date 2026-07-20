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

ARG SQLITE_VERSION
ARG SQLITE_YEAR
ARG SQLITE_ARCHIVE_VERSION
RUN sqlite_url="https://www.sqlite.org/${SQLITE_YEAR}/sqlite-autoconf-${SQLITE_ARCHIVE_VERSION}.tar.gz" \
    && curl -LsSf "$sqlite_url" -o /tmp/sqlite.tar.gz \
    && mkdir -p /tmp/sqlite-source \
    && tar -xzf /tmp/sqlite.tar.gz --strip-components=1 -C /tmp/sqlite-source \
    && cd /tmp/sqlite-source \
    && CPPFLAGS=-DSQLITE_ENABLE_DESERIALIZE ./configure --prefix=/opt/sqlite --disable-static \
    && make -j2 \
    && make install \
    && rm -rf /tmp/sqlite-source /tmp/sqlite.tar.gz

ENV LD_LIBRARY_PATH=/opt/sqlite/lib
ENV LD_PRELOAD=/opt/sqlite/lib/libsqlite3.so.0

RUN actual_version=$(python3 -c 'import sqlite3; print(sqlite3.sqlite_version)') \
    && test "$actual_version" = "$SQLITE_VERSION"

ENV PYTHONPATH=/workspace/src
WORKDIR /workspace
CMD ["sleep", "infinity"]
