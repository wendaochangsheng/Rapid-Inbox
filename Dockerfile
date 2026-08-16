# syntax=docker/dockerfile:1

FROM python:3.12-slim-bookworm AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        cmake \
        g++ \
        libicu-dev \
        libsqlite3-dev \
        libssl-dev \
        libunistring-dev \
        ninja-build \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

COPY pyproject.toml constraints-dev.txt README.md ./
COPY app ./app

RUN python -m venv /opt/rapid-inbox/venv \
    && /opt/rapid-inbox/venv/bin/pip install \
        --constraint constraints-dev.txt \
        .

COPY cpp/ingestd ./cpp/ingestd

RUN cmake \
        -S cpp/ingestd \
        -B /tmp/ingestd-build \
        -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
    && cmake --build /tmp/ingestd-build \
        --config Release \
        --parallel \
        --target rapid-inbox-ingestd


FROM python:3.12-slim-bookworm AS runtime

ARG RAPID_INBOX_UID=10001
ARG RAPID_INBOX_GID=10001

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        libicu72 \
        libsqlite3-0 \
        libssl3 \
        libunistring2 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid "${RAPID_INBOX_GID}" rapid-inbox \
    && useradd \
        --uid "${RAPID_INBOX_UID}" \
        --gid "${RAPID_INBOX_GID}" \
        --home-dir /var/lib/rapid-inbox \
        --no-create-home \
        --shell /usr/sbin/nologin \
        rapid-inbox

ENV HOME=/var/lib/rapid-inbox \
    PATH=/opt/rapid-inbox/venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY --from=builder /opt/rapid-inbox/venv /opt/rapid-inbox/venv
COPY --from=builder /tmp/ingestd-build/rapid-inbox-ingestd /usr/local/bin/rapid-inbox-ingestd
COPY app ./app
COPY pyproject.toml sqlite_schema.sql ./
COPY deploy/docker/entrypoint.sh /usr/local/bin/rapid-inbox-container
COPY deploy/docker/healthcheck.py /usr/local/lib/rapid-inbox/healthcheck.py

RUN chmod 0755 \
        /usr/local/bin/rapid-inbox-container \
        /usr/local/bin/rapid-inbox-ingestd \
        /usr/local/lib/rapid-inbox/healthcheck.py \
    && mkdir -p /var/lib/rapid-inbox \
    && chown rapid-inbox:rapid-inbox /var/lib/rapid-inbox

USER rapid-inbox:rapid-inbox

VOLUME ["/var/lib/rapid-inbox"]
EXPOSE 8000 2525
STOPSIGNAL SIGTERM

ENTRYPOINT ["/usr/local/bin/rapid-inbox-container"]
CMD ["run"]
