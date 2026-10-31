# Security

Please report security issues privately, not in a public issue: open a
[confidential issue on GitLab](https://gitlab.com/speedvault/speedvault/-/issues/new)
(tick "This issue is confidential"), or contact the maintainer directly. Include what you found, how to reproduce it, and the
version (commit) affected. You'll get an answer within a week.

SpeedVault has no TLS yet, and at most one password (`--requirepass-file`,
Redis's `requirepass`; no ACL users): run it on a network only trusted
clients reach (a Kubernetes namespace with a NetworkPolicy, a private
network), never exposed to the internet, and give it a password there
too. The password crosses the network in the clear, as Redis's does
without TLS. `/metrics`, `/healthz` and `/ready` need no password: they
show counts, never keys or values. Prefer `--requirepass-file` to
`--requirepass`, which anyone who can list the machine's processes can
read.
