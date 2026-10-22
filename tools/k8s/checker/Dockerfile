# kind_check.sh's checker: kcheck.py with migrate_check.py, redis-py 8.1.0
# and redis-cli (Debian bookworm's Redis 7.0.15), as the cluster-compat CI
# job has them; LettuceLoad (Lettuce 7.8.0, lettuce/build.sh) on a JDK; and
# memtier_benchmark 2.5.2, the load of the autoscaling scenarios. Built
# from tools/:
#
#   docker build -t speedvault-checker:dev -f tools/k8s/checker/Dockerfile tools

FROM debian:bookworm-slim AS memtier
RUN apt-get update -qq && apt-get install -y -qq --no-install-recommends \
        build-essential autoconf automake pkg-config libevent-dev zlib1g-dev git ca-certificates >/dev/null
RUN git clone -q --depth 1 --branch 2.5.2 https://github.com/RedisLabs/memtier_benchmark.git /src \
    && cd /src && test "$(git rev-parse HEAD)" = f3ff1793d39bf19077957e986f966206eb349de7 \
    && autoreconf -i >/dev/null 2>&1 && ./configure -q --disable-tls && make -s -j"$(nproc)" >/dev/null \
    && install -s memtier_benchmark /usr/local/bin/

FROM python:3.12-slim-bookworm
RUN apt-get update -qq && apt-get install -y -qq --no-install-recommends \
        redis-tools openjdk-17-jdk-headless curl libevent-2.1-7 libevent-extra-2.1-7 libevent-pthreads-2.1-7 >/dev/null \
    && rm -rf /var/lib/apt/lists/* \
    && redis-cli --version | grep -q ' 7\.0\.15' \
    && pip install --no-cache-dir --quiet redis==8.1.0
COPY --from=memtier /usr/local/bin/memtier_benchmark /usr/local/bin/
COPY compat/lettuce/ /app/compat/lettuce/
RUN mkdir -p /opt/lettuce && /app/compat/lettuce/build.sh /opt/lettuce > /opt/lettuce/classpath && memtier_benchmark --version | grep -q '2\.5\.2'
COPY compat/migrate_check.py /app/compat/
COPY k8s/kcheck.py /app/k8s/
USER 10001:10001
ENTRYPOINT ["python3", "/app/k8s/kcheck.py"]
