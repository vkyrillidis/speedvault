# The dbnode server and stuart, the steward.
#
#   docker build -t speedvault .
#   docker run --rm -p 7000:7000 speedvault                 # a node on 7000
#   docker run --rm -p 7000:7000 speedvault 7000 --timeout 300 --loglevel verbose
#   docker run --rm -p 7600:7600 --entrypoint stuart speedvault 7600 --state /var/lib/stuart/state
#
# docker-compose.yml runs a cluster: stuart and three nodes.
#
# Built on Debian bookworm, run on bookworm-slim (the same glibc). Both
# handle SIGTERM themselves, so `docker stop` is immediate.

FROM rust:1.92-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p dbnode -p stuart

FROM debian:bookworm-slim
COPY --from=build /src/target/release/dbnode /src/target/release/stuart /usr/local/bin/
# stuart's state file goes here (a volume in docker-compose.yml).
RUN mkdir -p /var/lib/stuart && chown 10001:10001 /var/lib/stuart
USER 10001:10001
EXPOSE 7000 7600
ENTRYPOINT ["dbnode"]
# A bare port listens on all interfaces, as a container needs.
CMD ["7000"]
