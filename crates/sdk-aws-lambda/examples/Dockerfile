FROM rust:1.94-bookworm AS build
WORKDIR /src
COPY Cargo.toml LICENSE.txt ./
COPY .cargo .cargo
COPY crates crates
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build -p temporalio-sdk-aws-lambda --example lambda_worker \
      --features otel,temporalio-common/vendored-protox && \
    cp target/debug/examples/lambda_worker /bootstrap

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates
COPY --from=build /bootstrap /var/runtime/bootstrap
ENTRYPOINT ["/var/runtime/bootstrap"]
