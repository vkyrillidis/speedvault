"""Known, intended differences between the target and the reference.

Maps a case name (fnmatch pattern, e.g. "select_1" or "command_*") to the
reason. A differing case that matches is reported as ALLOWED instead of
FAIL and doesn't fail the run; an allow-listed case that passes is reported
as a stale entry. Add entries here (or with --allow NAME=REASON) only for
deliberate decisions, with the reason.

Sets are selected by name with --allowlist (default: "rust").
"""

ALLOW = {
    # Differences we accept for the Rust dbnode compared with Redis 7.0.15.
    "rust": {
        "inline_binary_zero": "a NUL in an inline request is an ordinary byte, not the end of a C string "
                              "(resp crate docs: the one Redis parser quirk not copied)",
        "select_1": "only database 0 exists (README: SELECT 0)",
        "pipeline_select_midway": "only database 0 exists",
        # Example (uncomment once decided):
        # "client_setinfo": "we reply +OK like Redis 7.2 instead of 7.0's error",
    },
    # Redis vs Redis must be identical: nothing allowed.
    "none": {},
}
