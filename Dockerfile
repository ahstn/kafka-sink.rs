# --- Build Stage ---
FROM rust:1.72 AS builder

COPY . /usr/src/my_app
WORKDIR /usr/src/my_app
RUN cargo build --release

# --- Runtime Stage ---
FROM debian:buster-slim
WORKDIR /app

# Install SSL certificates and copy build artifacts
RUN apt-get update
RUN apt-get -y install git curl pkg-config unzip build-essential libssl-dev openssl libsasl2-dev cmake clang wget 
    
COPY --from=builder /usr/src/my_app/target/release/my_app /app/
COPY --from=builder /usr/src/my_app/.env.template /app/.env

# Run the application
CMD ["./my_app"]