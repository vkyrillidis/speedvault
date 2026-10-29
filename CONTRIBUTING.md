# Contributing

Thanks for helping. Issues and pull requests are welcome; for anything
large, open an issue first so we can agree on the approach.

## Building and testing

Linux or macOS, Rust 1.92 or later:

```sh
cargo build --release
cargo test                                   # unit and integration tests
cargo clippy --all-targets -- -D warnings    # must be clean
```

On Windows, build in WSL2, with `CARGO_TARGET_DIR` on the Linux file system
(e.g. `CARGO_TARGET_DIR=$HOME/target/speedvault`): a target directory on a
Windows drive is much slower.

## Compatibility with Redis

Replies must match Redis 7.0.15 byte for byte. The suites in `tools/compat`
check that against a real `redis-server` (Debian bookworm and Ubuntu 24.04
package 7.0.15) and need redis-py (`pip install redis==8.1.0`):

```sh
redis-server --port 6393 --save '' --appendonly no --daemonize yes
target/release/speedvault-server 7393 &
python3 tools/compat/difftest.py --ref 127.0.0.1:6393 --target 127.0.0.1:7393
tools/compat/cluster_compat.sh
tools/compat/cluster_difftest.sh
tools/compat/migrate_check.sh                # slots moving under load
```

A new command comes with difftest cases (`tools/compat/corpus.py`); an
intended difference from Redis goes in `tools/compat/allowlist.py` with its
reason. See [tools/compat/README.md](tools/compat/README.md).

## Changes

- Tests with the change: unit tests next to the code, integration tests in
  `*/tests/`. For a fix, a test that fails without it.
- Performance claims with numbers: `tools/bench` (see its README) against
  Redis, on a quiet machine, the server pinned to a core.
- Commits small and focused, with a message that says why.

## License

By contributing, you agree that your contributions are licensed under the
project's terms: MIT OR Apache-2.0, at the user's option (see the README).
