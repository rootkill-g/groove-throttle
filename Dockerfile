FROM rust:1.90.0-slim-trixie AS builder

WORKDIR /usr/src/app

COPY Cargo.toml Cargo.lock ./
COPY . .

RUN apt-get update && apt-get install -y libssl-dev pkg-config && rm -rf /var/lib/apt/lists/*

RUN cargo build --release


FROM debian:trixie-slim

COPY --from=builder /usr/src/app/target/release/load-reducer-poc /usr/local/bin/rust_web_app

EXPOSE 8000

CMD ["rust_web_app"]
