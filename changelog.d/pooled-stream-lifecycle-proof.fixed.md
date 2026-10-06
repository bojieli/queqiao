Preserve bounded pooled-connection path proof when an older stream finishes
or is canceled with the normal zero code. A late stream close no longer makes
the next short flow pay for an unnecessary path probe; timeouts, other
failures, and stale proof still require validation, and failed connections
are retired. Distinguish a frame missing its entire payload from clean stream
EOF so truncated frames still invalidate path proof.
