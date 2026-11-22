#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
MOBILE_DIR=$(CDPATH='' cd -- "$SCRIPT_DIR/.." && pwd)
CORE_DIR="$MOBILE_DIR/core"
RUNTIME_LOCK="$MOBILE_DIR/runtime-dependencies.lock"
BUILD_LOCK="$MOBILE_DIR/build-tools.lock"
ANDROID_BUILD_FILE="$MOBILE_DIR/android/build.gradle"
GRADLE_WRAPPER="$MOBILE_DIR/android/gradle/wrapper/gradle-wrapper.properties"
GRADLE_VERIFICATION="$MOBILE_DIR/android/gradle/verification-metadata.xml"
CI_WORKFLOW="$MOBILE_DIR/../.github/workflows/ci.yml"

AUDIT_DIR=$(mktemp -d "${TMPDIR:-/tmp}/queqiao-dependency-audit.XXXXXX")
trap 'rm -rf "$AUDIT_DIR"' EXIT HUP INT TERM

cd "$CORE_DIR"
go mod verify

validate_lock_metadata() {
    lock_file=$1
    while IFS='|' read -r component license_id maintenance_source digest extra; do
        case "$component" in
            ''|'#'*) continue ;;
            *@*) ;;
            *)
                echo "invalid component reference in $lock_file: $component" >&2
                exit 1
                ;;
        esac
        case "$license_id" in
            MIT|BSD-3-Clause|Apache-2.0) ;;
            *)
                echo "unapproved license for $component: $license_id" >&2
                exit 1
                ;;
        esac
        case "$maintenance_source" in
            https://*) ;;
            *)
                echo "missing maintenance source for $component" >&2
                exit 1
                ;;
        esac
        case "$digest" in
            '') ;;
            sha256:*)
                checksum=${digest#sha256:}
                case "$checksum" in
                    *[!0-9a-f]*|'')
                        echo "invalid SHA-256 pin for $component" >&2
                        exit 1
                        ;;
                esac
                if [ "${#checksum}" -ne 64 ]; then
                    echo "invalid SHA-256 pin for $component" >&2
                    exit 1
                fi
                ;;
            *)
                echo "invalid artifact digest for $component" >&2
                exit 1
                ;;
        esac
        if [ -n "$extra" ]; then
            echo "too many fields for $component in $lock_file" >&2
            exit 1
        fi
    done <"$lock_file"
}

validate_lock_metadata "$RUNTIME_LOCK"
validate_lock_metadata "$BUILD_LOCK"

# Go's module graph includes upstream projects' test and repository tooling.
# Only modules reached by compiled packages can enter the application binary,
# so derive the shipped graph from go list -deps and compare it exactly.
go list -deps -f '{{with .Module}}{{if and .Version (ne .Path "github.com/bojieli/queqiao")}}{{with .Replace}}{{.Path}}@{{.Version}}{{else}}{{.Path}}@{{.Version}}{{end}}{{end}}{{end}}' . |
    sed '/^$/d' | sort -u >"$AUDIT_DIR/runtime.actual"
sed '/^[[:space:]]*#/d; /^[[:space:]]*$/d' "$RUNTIME_LOCK" |
    cut -d '|' -f 1 | sed '/^golang.org\/x\/mobile@/d' | sort -u >"$AUDIT_DIR/runtime.expected"
if ! diff -u "$AUDIT_DIR/runtime.expected" "$AUDIT_DIR/runtime.actual"; then
    echo "mobile runtime dependency graph differs from the reviewed lock" >&2
    exit 1
fi

# Resolve the command graph before checking license files. x/mobile is listed
# in the runtime lock because gomobile links its binding support into the apps,
# but it is not reachable from `go list -deps .` until the tool graph has been
# downloaded on a clean machine.
go list -deps -f '{{with .Module}}{{if .Version}}{{.Path}}@{{.Version}}{{end}}{{end}}' \
    golang.org/x/mobile/cmd/gomobile golang.org/x/mobile/cmd/gobind |
    sed '/^$/d' | sort -u >"$AUDIT_DIR/tools.actual"
sed '/^[[:space:]]*#/d; /^[[:space:]]*$/d' "$BUILD_LOCK" |
    cut -d '|' -f 1 | sed -n '/^golang.org\/x\//p' | sort -u >"$AUDIT_DIR/tools.expected"
if ! diff -u "$AUDIT_DIR/tools.expected" "$AUDIT_DIR/tools.actual"; then
    echo "gomobile build-tool graph differs from the reviewed lock" >&2
    exit 1
fi

while IFS='|' read -r module_ref _license_id _maintenance_source _digest; do
    case "$module_ref" in
        ''|'#'*) continue ;;
    esac
    module_name=${module_ref%@*}
    module_version=${module_ref##*@}
    module_dir=$(go list -m -f '{{.Dir}}' "$module_name@$module_version")
    if ! find "$module_dir" -maxdepth 1 -type f \( -iname 'LICENSE*' -o -iname 'COPYING*' \) | grep -q .; then
        echo "module has no top-level license file: $module_ref" >&2
        exit 1
    fi
done <"$RUNTIME_LOCK"

while IFS='|' read -r module_ref _license_id _maintenance_source _digest; do
    case "$module_ref" in
        golang.org/*) ;;
        *) continue ;;
    esac
    module_name=${module_ref%@*}
    module_version=${module_ref##*@}
    module_dir=$(go list -m -f '{{.Dir}}' "$module_name@$module_version")
    if ! find "$module_dir" -maxdepth 1 -type f \( -iname 'LICENSE*' -o -iname 'COPYING*' \) | grep -q .; then
        echo "build module has no top-level license file: $module_ref" >&2
        exit 1
    fi
done <"$BUILD_LOCK"

android_plugin_version=$(sed -n 's/.*com\.android\.application" version "\([^"]*\)".*/\1/p' "$ANDROID_BUILD_FILE")
gradle_version=$(sed -n 's|.*gradle-\([0-9][0-9.]*\)-bin\.zip|\1|p' "$GRADLE_WRAPPER")
android_action_ref=$(sed -n 's/.*android-actions\/setup-android@\([0-9a-f][0-9a-f]*\).*/\1/p' "$CI_WORKFLOW" | sort -u)
android_cmdline_version=$(sed -n 's/.*cmdline-tools-version: "\([0-9][0-9]*\)".*/\1/p' "$CI_WORKFLOW" | sort -u)
if ! grep -Fqx "com.android.tools.build:gradle@$android_plugin_version|Apache-2.0|https://android.googlesource.com/platform/tools/base" "$BUILD_LOCK"; then
    echo "Android Gradle Plugin version is not in the reviewed build-tool lock" >&2
    exit 1
fi
if ! grep -Fqx "gradle@$gradle_version|Apache-2.0|https://github.com/gradle/gradle" "$BUILD_LOCK"; then
    echo "Gradle wrapper version is not in the reviewed build-tool lock" >&2
    exit 1
fi
if [ "${#android_action_ref}" -ne 40 ] ||
   ! grep -Fqx "android-actions/setup-android@$android_action_ref|MIT|https://github.com/android-actions/setup-android" "$BUILD_LOCK"; then
    echo "Android SDK setup action is not commit-pinned in the reviewed build-tool lock" >&2
    exit 1
fi
if [ -z "$android_cmdline_version" ] ||
   ! grep -Fqx "android-command-line-tools@$android_cmdline_version|Apache-2.0|https://developer.android.com/studio" "$BUILD_LOCK"; then
    echo "Android command-line tools are not pinned in the reviewed build-tool lock" >&2
    exit 1
fi
swiftlint_count=$(grep -c '^swiftlint@' "$BUILD_LOCK" || true)
if [ "$swiftlint_count" -ne 1 ]; then
    echo "SwiftLint is not uniquely pinned in the reviewed build-tool lock" >&2
    exit 1
fi
if ! grep -Eq '^distributionSha256Sum=[0-9a-f]{64}$' "$GRADLE_WRAPPER"; then
    echo "Gradle distribution is missing its SHA-256 pin" >&2
    exit 1
fi
if ! grep -Fq '<verify-metadata>true</verify-metadata>' "$GRADLE_VERIFICATION" ||
   ! grep -Fq 'name="gradle" version="9.3.1"' "$GRADLE_VERIFICATION"; then
    echo "Gradle dependency verification metadata is missing or does not cover AGP 9.3.1" >&2
    exit 1
fi

echo "Mobile runtime and build-tool dependencies match the reviewed allowlists."
