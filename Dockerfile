FROM rust:1.70.0 as build-env
WORKDIR /app
COPY . /app
RUN cargo build --release --bin s3ite --features binary

FROM gcr.io/distroless/cc
COPY --from=build-env /app/target/release/s3ite /
ENTRYPOINT ["./s3ite"]
CMD ["--help"]