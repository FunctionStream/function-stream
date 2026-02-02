FROM rust:1-bookworm AS builder

RUN apt-get update && apt-get install -y \
    cmake \
    make \
    clang \
    libclang-dev \
    python3 \
    python3-venv \
    python3-pip \
    libssl-dev \
    pkg-config \
    libsasl2-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

ENV PROTOC=/usr/bin/protoc

WORKDIR /build

COPY Makefile Cargo.toml Cargo.lock ./
COPY protocol ./protocol
COPY cli ./cli
COPY src ./src
COPY python ./python
COPY wit ./wit
COPY config.yaml ./

RUN python3 -m venv .venv \
    && .venv/bin/pip install --upgrade pip \
    && .venv/bin/pip install componentize-py \
    && .venv/bin/pip install -e python/functionstream-api

RUN cd python/functionstream-runtime && make build

RUN make build-full

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /build/target/release/function-stream bin/
COPY --from=builder \
    /build/python/functionstream-runtime/target/functionstream-python-runtime.wasm \
    data/cache/python-runner/
COPY config.yaml conf/
RUN sed -i 's/127.0.0.1/0.0.0.0/' conf/config.yaml

EXPOSE 8080

ENTRYPOINT ["./bin/function-stream"]
