#!/usr/bin/env bash
# Runs the Android project's Gradle build with the given arguments, and runs
# it again -- up to three attempts, with backoff -- when it failed fetching a
# dependency. Maven Central answers bursts of CI traffic with HTTP 429, and
# Gradle disables a repository for the rest of a build after one such error,
# so a retry has to be a fresh invocation. A failure that is not a fetch
# (a compile error, lint, a dependency-verification mismatch) is not retried.
set -euo pipefail

SCRIPT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
cd "$SCRIPT_DIR/../android"

# Transport-level failures only. "Could not find" (a 404) and verification
# failures are deterministic and deliberately absent.
transient='Could not (GET|HEAD|download)|Received status code (408|429|5[0-9][0-9])|is disabled due to earlier error|Connection reset|timed out|Remote host terminated|UnknownHostException|SSLHandshakeException'

log=$(mktemp)
trap 'rm -f "$log"' EXIT

attempts=3
delay=15
for ((attempt = 1; ; attempt++)); do
    if ./gradlew --no-daemon --dependency-verification strict \
        --init-script "$SCRIPT_DIR/prefetch-configurations.init.gradle" \
        "$@" 2>&1 | tee "$log"; then
        exit 0
    fi
    if grep -q 'Dependency verification failed' "$log" || ! grep -Eq "$transient" "$log"; then
        echo "Gradle failed for a reason other than fetching a dependency; not retrying" >&2
        exit 1
    fi
    if ((attempt >= attempts)); then
        echo "Gradle failed fetching dependencies $attempts times" >&2
        exit 1
    fi
    echo "Gradle failed fetching dependencies (attempt $attempt of $attempts); retrying in ${delay}s" >&2
    sleep "$delay"
    delay=$((delay * 3))
done
