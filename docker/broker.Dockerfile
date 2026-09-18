FROM rust:1.90-bookworm AS builder

WORKDIR /app

COPY . .

RUN cargo build --release -p broker

FROM debian:bookworm-slim AS runtime

COPY --from=builder /app/target/release/broker /usr/local/bin/broker

ENTRYPOINT ["/usr/local/bin/broker"]
