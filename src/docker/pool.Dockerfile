FROM ubuntu:24.04
COPY pool /usr/local/bin/pool
COPY pool /usr/local/bin/mining-pool
ENTRYPOINT ["/usr/local/bin/pool"]
