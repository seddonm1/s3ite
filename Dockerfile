FROM rust:1.96.0 as build-env
RUN cargo install --locked cargo-multivers --version 0.11.1
WORKDIR /app
COPY . /app
RUN cargo multivers --out-dir /app/dist -- --bin s3ite --features binary

FROM gcr.io/distroless/cc
COPY --from=build-env /app/dist/s3ite /
ENTRYPOINT ["./s3ite"]
CMD ["--help"]
