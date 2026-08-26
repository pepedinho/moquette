# Use Debian slim for compatibility with glibc on ARMv7
FROM debian:bookworm-slim

WORKDIR /app

# Copy pre-compiled binary from CI build step
COPY target/armv7-unknown-linux-gnueabihf/release/moquette /app/moquette

# Default MQTT port
EXPOSE 1883

ENTRYPOINT ["/app/moquette"]
