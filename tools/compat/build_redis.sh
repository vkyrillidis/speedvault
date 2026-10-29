#!/bin/bash
# build_redis.sh [DIR]: builds the reference Redis of the compat tests,
# 8.10.2, from its release tag (the commit pinned) into DIR (default
# .redis): DIR/bin/redis-server, redis-cli, redis-benchmark. Does nothing
# if DIR has it already (CI caches it). Needs git, make and a C compiler.
set -euo pipefail
TAG=8.10.2
COMMIT=498ecd0d6d007db11ddb3aea9428552598a78622
DIR="$(realpath -m "${1:-.redis}")"
if "$DIR/bin/redis-server" --version 2>/dev/null | grep -q "v=$TAG "; then
  exit 0
fi
SRC="$(mktemp -d)"
trap 'rm -rf "$SRC"' EXIT
git clone --quiet --depth 1 --branch "$TAG" https://github.com/redis/redis.git "$SRC"
if [ "$(git -C "$SRC" rev-parse HEAD)" != "$COMMIT" ]; then
  echo "build_redis.sh: tag $TAG is not commit $COMMIT" >&2
  exit 1
fi
make -C "$SRC" -j"$(nproc)" >/dev/null
make -C "$SRC" install PREFIX="$DIR" >/dev/null
"$DIR/bin/redis-server" --version
