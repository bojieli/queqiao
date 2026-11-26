#!/bin/sh
# Downloads every Gradle dependency of the Android project, retrying the whole
# resolution up to three times with backoff. Maven Central answers bursts of CI
# traffic with 429s, and Gradle disables a repository for the rest of a build
# after one such error, so a retry has to start a fresh Gradle invocation.
# Run this before any step that builds or tests; those steps then pass
# --offline and never touch the network.
set -eu

SCRIPT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
cd "$SCRIPT_DIR/../android"

attempts=3
delay=15
attempt=1
while :; do
    if ./gradlew --no-daemon --dependency-verification strict \
        --init-script "$SCRIPT_DIR/prefetch-android-dependencies.init.gradle" \
        queqiaoPrefetchDependencies; then
        exit 0
    fi
    if [ "$attempt" -ge "$attempts" ]; then
        echo "Gradle dependency resolution failed $attempts times" >&2
        exit 1
    fi
    echo "Gradle dependency resolution failed (attempt $attempt of $attempts); retrying in ${delay}s" >&2
    sleep "$delay"
    attempt=$((attempt + 1))
    delay=$((delay * 3))
done
