"""Known, intended differences between the target and the reference.

Maps a case name (fnmatch pattern, e.g. "select_1" or "command_*") to the
reason. A differing case that matches is reported as ALLOWED instead of
FAIL and doesn't fail the run; an allow-listed case that passes is reported
as a stale entry. Add entries here (or with --allow NAME=REASON) only for
deliberate decisions, with the reason.

Sets are selected by name with --allowlist (default: "rust").
"""

ALLOW = {
    # Differences we accept for SpeedVault compared with Redis 8.10.2.
    "rust": {
        "inline_binary_zero": "a NUL in an inline request is an ordinary byte, not the end of a C string "
                              "(resp crate docs: the one Redis parser quirk not copied)",
        "select_1": "only database 0 exists (README: SELECT 0)",
        "pipeline_select_midway": "only database 0 exists",
        "proto_star1_X_waits": "Redis 8.10.2 never answers '*1\\r\\nX' once the line ends in a later read (the "
                               "same bytes at once get the error and a close): we reply the error, as Redis 7 did",
        "proto_too_big_bulk_count": "Redis 8.10.2 waits forever for a bulk count of 70000 digits (even a million); "
                                    "we refuse it past 64 KiB, as Redis 7 did",
        "auth_pipelined_wrong*": "Redis 8.10.2 sends no reply at all to a failed AUTH while replies are pending "
                                 "(addAuthErrReply), so the client's replies no longer line up with its requests: "
                                 "we reply -WRONGPASS",
        "auth_limit_split": "Redis 8.10.2 never answers a bad bulk length whose line ends in a later read (any bad "
                            "length, with or without a password; the same bytes at once get the error and a close): we "
                            "reply the error, as Redis 7.0.15 does",
    },
    # Redis vs Redis must be identical: nothing allowed.
    "none": {},
}
