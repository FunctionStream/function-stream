# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# syntax=docker/dockerfile:1

FROM rust:1-bookworm AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    clang \
    libclang-dev \
    libssl-dev \
    libcurl4-openssl-dev \
    pkg-config \
    libsasl2-dev \
    protobuf-compiler \
    python3-full \
    python3-pip \
    python3-venv \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

COPY Cargo.toml Cargo.lock Makefile ./
COPY scripts ./scripts
COPY python ./python
COPY wit ./wit

COPY protocol ./protocol
COPY cli ./cli
COPY src ./src
COPY conf ./conf
COPY bin ./bin

RUN bash scripts/setup.sh


RUN make dist

FROM debian:bookworm-slim AS runtime

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    libsasl2-2 \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -g 1000 appgroup && \
    useradd -m -u 1000 -g appgroup -s /bin/bash appuser

WORKDIR /app

COPY --from=builder --chown=appuser:appgroup /workspace/target/release/function-stream bin/
COPY --from=builder --chown=appuser:appgroup /workspace/bin/start-server.sh bin/

RUN chmod +x bin/function-stream bin/start-server.sh

COPY --from=builder --chown=appuser:appgroup \
    /workspace/python/functionstream-runtime/target/functionstream-python-runtime.wasm \
    data/cache/python-runner/
COPY --from=builder --chown=appuser:appgroup /workspace/conf/config.yaml conf/

RUN sed -i 's/127.0.0.1/0.0.0.0/' conf/config.yaml

USER appuser
EXPOSE 8080
VOLUME ["/app/data", "/app/logs"]

ENTRYPOINT ["./bin/start-server.sh"]
CMD ["--config", "conf/config.yaml"]
