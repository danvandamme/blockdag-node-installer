FROM ubuntu:24.04
COPY bdag /usr/local/bin/bdag
COPY nodeworker /usr/local/bin/nodeworker
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
ENTRYPOINT ["/bin/sh", "/usr/local/bin/entrypoint.sh"]
