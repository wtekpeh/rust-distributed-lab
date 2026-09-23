FROM rust:1.90-bookworm AS builder

WORKDIR /app

COPY . .

RUN cargo build --release -p producer


FROM debian:bookworm-slim AS runtime

COPY --from=builder /app/target/release/producer /usr/local/bin/producer

ENTRYPOINT ["/usr/local/bin/producer"]