FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY .image-build/ /

EXPOSE 8383
VOLUME ["/data"]

ENTRYPOINT ["/usr/local/bin/reduct-bridge"]
