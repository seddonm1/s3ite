FROM rust:1.96.0 AS build-env
RUN cargo install --locked cargo-multivers --version 0.11.1
WORKDIR /app
COPY . /app
RUN cargo multivers \
    --exclude-cpu-features lor,pan,pmuv3,ras,spe,vh \
    --out-dir /app/dist \
    -- --bin s3ite

FROM gcr.io/distroless/cc
COPY --from=build-env /app/dist/s3ite /
ENTRYPOINT ["./s3ite"]
CMD ["--help"]
