FROM rust:1.90-bookworm AS builder

WORKDIR /app

COPY . .

RUN cargo build --release -p consumer


FROM debian:bookworm-slim AS runtime

COPY --from=builder /app/target/release/consumer /usr/local/bin/consumer

ENTRYPOINT ["/usr/local/bin/consumer"]