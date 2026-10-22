# Security

Please report security issues privately, not in a public issue: use the
repository host's private vulnerability reporting, or contact the
maintainer directly. Include what you found, how to reproduce it, and the
version (commit) affected. You'll get an answer within a week.

SpeedVault, like Redis, has no authentication or TLS yet: run it on a
network only trusted clients reach (a Kubernetes namespace with a
NetworkPolicy, a private network), never exposed to the internet.
