#!/bin/sh
set -eu

if [ "$#" -ne 1 ]; then
    echo "usage: audit-mobile-binary.sh AAR_OR_XCFRAMEWORK" >&2
    exit 2
fi

SCRIPT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
MOBILE_DIR=$(CDPATH='' cd -- "$SCRIPT_DIR/.." && pwd)
PROJECT_DIR=$(CDPATH='' cd -- "$MOBILE_DIR/.." && pwd)
LOCK_FILE="$MOBILE_DIR/runtime-dependencies.lock"
GO_VERSION=$(sed -n 's/^go //p' "$MOBILE_DIR/core/go.mod")
ARTIFACT=$1

AUDIT_DIR=$(mktemp -d "${TMPDIR:-/tmp}/queqiao-mobile-binary-audit.XXXXXX")
trap 'rm -rf "$AUDIT_DIR"' EXIT HUP INT TERM

case "$ARTIFACT" in
    *.aar)
        unzip -q "$ARTIFACT" \
            'jni/arm64-v8a/libgojni.so' \
            'jni/x86_64/libgojni.so' \
            -d "$AUDIT_DIR"

        READOBJ=
        for CANDIDATE in "$ANDROID_NDK_HOME"/toolchains/llvm/prebuilt/*/bin/llvm-readobj; do
            if [ -x "$CANDIDATE" ]; then
                READOBJ=$CANDIDATE
                break
            fi
        done
        if [ -z "$READOBJ" ]; then
            echo "Android NDK llvm-readobj is required to audit ELF alignment" >&2
            exit 1
        fi
        for ABI in arm64-v8a x86_64; do
            LIBRARY="$AUDIT_DIR/jni/$ABI/libgojni.so"
            if ! "$READOBJ" --program-headers "$LIBRARY" | awk '
                /Type: PT_LOAD/ { load = 1; next }
                load && /Alignment:/ {
                    count++
                    if (($2 + 0) < 16384) bad = 1
                    load = 0
                }
                END { exit count == 0 || bad }
            '; then
                echo "Android $ABI mobile core is not 16 KiB ELF-aligned" >&2
                exit 1
            fi
        done

        go version -m "$AUDIT_DIR/jni/arm64-v8a/libgojni.so" >"$AUDIT_DIR/buildinfo"
        # Go prints a replacement on the line following its import identity.
        awk '
            $1 == "dep" { if (pending) print pending; pending = ""; if ($2 != "github.com/bojieli/queqiao" && $2 != "github.com/bojieli/queqiao/mobile/core") pending = $2 "@" $3 }
            $1 == "=>" && pending { pending = $2 "@" $3 }
            END { if (pending) print pending }
        ' \
            "$AUDIT_DIR/buildinfo" | sort -u >"$AUDIT_DIR/actual"
        grep -Eq '^[[:space:]]*build[[:space:]]+-trimpath=true$' "$AUDIT_DIR/buildinfo" || {
            echo "Android mobile core was built without -trimpath" >&2
            exit 1
        }
        grep -Fq ": go$GO_VERSION" "$AUDIT_DIR/buildinfo" || {
            echo "Android mobile core was not built with required Go $GO_VERSION" >&2
            exit 1
        }
        ;;
    *.xcframework)
        FRAMEWORK_BINARY="$ARTIFACT/ios-arm64/Mobilecore.framework/Mobilecore"
        lipo "$FRAMEWORK_BINARY" -thin arm64 -output "$AUDIT_DIR/mobilecore.a"
        (cd "$AUDIT_DIR" && ar -x mobilecore.a)
        strings "$AUDIT_DIR/go.o" >"$AUDIT_DIR/buildinfo"
        # The object contains both identities for a replaced module. Keep the
        # actual pinned source when comparing to the reviewed lock.
        (cd "$MOBILE_DIR/core" && go list -m -f '{{if .Replace}}{{.Path}}@{{.Version}}{{end}}' all) |
            sed '/^$/d' >"$AUDIT_DIR/replaced"
        awk 'previous ~ /^[A-Za-z0-9.-]+\.[A-Za-z0-9.-]+\// && $0 ~ /^v[0-9]/ { print previous "@" $0 } { previous = $0 }' \
            "$AUDIT_DIR/buildinfo" |
            sed '/^github.com\/bojieli\/queqiao@/d; /^github.com\/bojieli\/queqiao\/mobile\/core@/d' |
            awk 'FILENAME == ARGV[1] { skip[$0] = 1; next } !($0 in skip)' "$AUDIT_DIR/replaced" - |
            sort -u >"$AUDIT_DIR/actual"
        grep -Fqx -- '-trimpath=true' "$AUDIT_DIR/buildinfo" || {
            echo "iOS mobile core was built without -trimpath" >&2
            exit 1
        }
        grep -Fqx -- "go$GO_VERSION" "$AUDIT_DIR/buildinfo" || {
            echo "iOS mobile core was not built with required Go $GO_VERSION" >&2
            exit 1
        }
        ;;
    *)
        echo "unsupported mobile artifact: $ARTIFACT" >&2
        exit 2
        ;;
esac

if grep -Fq "$PROJECT_DIR" "$AUDIT_DIR/buildinfo"; then
    echo "mobile artifact leaks the local checkout path" >&2
    exit 1
fi

sed '/^[[:space:]]*#/d; /^[[:space:]]*$/d' "$LOCK_FILE" |
    cut -d '|' -f 1 | sort -u >"$AUDIT_DIR/expected"
if ! diff -u "$AUDIT_DIR/expected" "$AUDIT_DIR/actual"; then
    echo "compiled mobile dependency graph differs from the reviewed runtime lock" >&2
    exit 1
fi

echo "Compiled mobile dependency graph and trim-path setting verified."
