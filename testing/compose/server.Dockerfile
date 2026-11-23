# The server image for the test cluster: the fetched server binary (see ../server.json)
# copied in beside this file. The binary's licence travels with the release asset.
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -g 1000 hoarddb && \
    adduser -u 1000 -G hoarddb -s /bin/sh -D hoarddb

COPY hoardDB-server /usr/local/bin/hoarddb-server

# Data directory. The code default is ./data; the image
# pins /data explicitly so container data keeps landing on the volume.
ENV HOARDDB_DATA_DIR=/data
# Build-time assertion: the image keeps /data. Fails the build if the ENV is dropped.
RUN test "$HOARDDB_DATA_DIR" = "/data"
RUN mkdir -p /data && chown hoarddb:hoarddb /data

# The binary listens on loopback by default, and loopback inside a
# container is unreachable through `docker run -p`. The image therefore opts in
# to every interface; what is
# reachable from outside is decided by which ports you publish. A clustered
# node overrides HOARDDB_LISTEN with its own address (see cluster.yml).
ENV HOARDDB_LISTEN=0.0.0.0:4433
ENV HOARDDB_TCP_LISTEN=0.0.0.0:7433
ENV HOARDDB_METRICS_LISTEN=0.0.0.0:9090

USER hoarddb
WORKDIR /data

# 7433 is the client/driver port (the server's default); 4434 is
# the internode port (ring address port 4433 + 1). 4433 itself is only a ring
# identity - nothing binds it - so it is not exposed. `docker run -p 7433:7433`
# is all a single node needs.
EXPOSE 7433 4434

ENTRYPOINT ["hoarddb-server"]
