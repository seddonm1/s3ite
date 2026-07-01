FROM rust:1.96.0 AS build-env

ARG TARGETARCH

ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=musl-gcc \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=musl-gcc \
    CFLAGS_aarch64_unknown_linux_musl=-mno-outline-atomics \
    CC_aarch64_unknown_linux_musl=musl-gcc \
    CC_x86_64_unknown_linux_musl=musl-gcc

RUN apt-get update \
    && apt-get install --yes --no-install-recommends musl-tools \
    && rm -rf /var/lib/apt/lists/*

RUN cargo install --locked cargo-multivers --version 0.12.0
WORKDIR /app
COPY . /app
RUN set -eux; \
    arch="${TARGETARCH:-$(uname -m)}"; \
    case "$arch" in \
        amd64|x86_64) rust_target=x86_64-unknown-linux-musl ;; \
        arm64|aarch64) rust_target=aarch64-unknown-linux-musl ;; \
        *) echo "unsupported target architecture: $arch" >&2; exit 1 ;; \
    esac; \
    rustup target add "$rust_target"; \
    cargo multivers \
        --target "$rust_target" \
        --profile release \
        --exclude-cpu-features crt-static,lor,pan,pmuv3,ras,spe,vh \
        --out-dir /app/dist \
        -- --bin s3ite

RUN mkdir -p /scratch/data /scratch/tmp \
    && chmod 1777 /scratch/tmp

FROM scratch
COPY --from=build-env /scratch/data /data
COPY --from=build-env /scratch/tmp /tmp
COPY --from=build-env /app/dist/s3ite /s3ite
ENTRYPOINT ["/s3ite"]
CMD ["--help"]
