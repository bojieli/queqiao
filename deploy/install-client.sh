#!/bin/sh
set -eu

# One-command Queqiao desktop client install for macOS and Linux. It enrolls
# one or more invitations, writes the multi-provider manifest, installs a
# per-user service that starts at login (launchd on macOS, a systemd --user
# unit with lingering on Linux), and verifies each SOCKS5 listener.
#
# Run it as the account that will use the tunnel, not as root: the profile is a
# private key file owned by that user, and the service is a user agent.
#
# Adding a provider later is the same command with only the new invitation.
# Existing manifest entries and their listener ports are preserved.

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH='' cd -- "$script_dir/.." && pwd)

home=${HOME:?HOME must be set}
prefix=$home/.queqiao
# Left empty here and resolved once the platform is known: `queqiaod enroll`
# uses the platform configuration directory, and an installer that chose a
# different one would leave hand-enrolled and script-enrolled profiles in two
# places with no hint that either existed.
config_dir=
base_port=12080
metrics_listen=127.0.0.1:12090
local_address=auto
path_profile=
device_name=
log_level=info
label=me.01.queqiao.client
service_name=queqiao-client
binary_source=
verify=true
egress_check=true
start_service=true
dry_run=false

tab=$(printf '\t')
work_dir=
staged_binary=
binary_origin=

usage() {
	cat <<'EOF'
Usage: deploy/install-client.sh --invite 'queqiao://enroll/...' [--invite ...] [options]

Enroll one or more provider invitations, install queqiaod as a per-user
service that starts at login, and verify the resulting SOCKS5 listeners.

Invitations:
  --invite URI             a single-use invitation; repeat for several providers
  --invite NAME=URI        the same, with an explicit manifest name
  URI                      an invitation may also be given positionally

Options:
  --base-port PORT         first loopback SOCKS5 port (default 12080, matching
                           deploy/clash-queqiao.yaml); later providers take the
                           next free ports
  --local-address VALUE    outer source for enrollment and traffic: auto, an IP,
                           or if:NAME (default auto). Use if:en0 when Clash TUN
                           owns the default route or two uplinks are active.
  --path-profile NAME      path policy: wan-shared-bottleneck (default) or
                           dc-long-haul. The second is experimental and is for a
                           long hop between two regions you operate; measure the
                           path first, per docs/DEPLOYING-DC-PROFILE.md. An
                           unknown name fails the install rather than falling
                           back to the default.
  --device-name NAME       device label shown to the provider (default hostname)
  --metrics-listen ADDR    loopback metrics address (default 127.0.0.1:12090)
  --log-level LEVEL        debug, info, warn, or error (default info)
  --config-dir DIR         profiles and manifest (default: the same directory
                           `queqiaod enroll` uses -- ~/Library/Application
                           Support/queqiao on macOS, ~/.config/queqiao on Linux)
  --prefix DIR             binary install prefix (default ~/.queqiao)
  --label NAME             macOS LaunchAgent label (default me.01.queqiao.client)
  --service-name NAME      Linux systemd --user unit name (default queqiao-client)
  --binary PATH            install this queqiaod instead of building one
  --no-start               write the service definition without loading it
  --no-egress-check        skip the outbound request through the tunnel
  --no-verify              skip listener and egress verification entirely
  --dry-run                print the plan and exit without changing anything
  -h, --help               show this help

Re-running the script updates the install in place. Changing --config-dir,
--prefix, --label, or --service-name relocates it: the service is stopped,
every enrolled profile moves with it, and the superseded definition and binary
are removed. Profiles are moved rather than re-enrolled because an invitation
is single-use and the device key cannot be reissued.

The binary is taken from --binary, then ./queqiaod in the repository root,
then a local `go build ./cmd/queqiaod`.
EOF
}

die() {
	echo "install-client.sh: $*" >&2
	exit 1
}

usage_error() {
	echo "install-client.sh: $*" >&2
	echo "Run deploy/install-client.sh --help for usage." >&2
	exit 2
}

next_value() {
	if [ "$1" -lt 2 ]; then
		usage_error "$2 requires a value"
	fi
}

work_dir=$(mktemp -d)
cleanup() {
	rm -rf "$work_dir"
}
trap cleanup 0 HUP INT TERM
pending=$work_dir/pending
entries=$work_dir/entries
: >"$pending"
: >"$entries"

# An invitation may be written as NAME=URI. Splitting on the first '=' is
# unambiguous because a name is restricted to slug characters here, while the
# URI always begins with its scheme.
#
# The URI is stored first because a tab is IFS whitespace: a leading empty
# field would be stripped on read and the invitation itself would be parsed as
# the name. An unnamed provider leaves the trailing field empty instead.
record_invite() {
	case $1 in
	queqiao://*)
		printf '%s\t\n' "$1" >>"$pending"
		;;
	[A-Za-z0-9]*=queqiao://*)
		invite_name=${1%%=*}
		case $invite_name in
		*[!A-Za-z0-9._-]*)
			die "a provider name may use letters, digits, dot, underscore, and dash only: $invite_name"
			;;
		esac
		printf '%s\t%s\n' "${1#*=}" "$invite_name" >>"$pending"
		;;
	*)
		die "not an invitation URI or NAME=URI pair: $1"
		;;
	esac
}

while [ "$#" -gt 0 ]; do
	case $1 in
	--invite)
		next_value "$#" "$1"
		record_invite "$2"
		shift
		;;
	--base-port)
		next_value "$#" "$1"
		base_port=$2
		shift
		;;
	--path-profile)
		[ $# -ge 2 ] || die "--path-profile needs a value."
		path_profile=$2
		shift 2
		;;
	--local-address)
		next_value "$#" "$1"
		local_address=$2
		shift
		;;
	--device-name)
		next_value "$#" "$1"
		device_name=$2
		shift
		;;
	--metrics-listen)
		next_value "$#" "$1"
		metrics_listen=$2
		shift
		;;
	--log-level)
		next_value "$#" "$1"
		log_level=$2
		shift
		;;
	--config-dir)
		next_value "$#" "$1"
		config_dir=$2
		shift
		;;
	--prefix)
		next_value "$#" "$1"
		prefix=$2
		shift
		;;
	--label)
		next_value "$#" "$1"
		label=$2
		shift
		;;
	--service-name)
		next_value "$#" "$1"
		service_name=$2
		shift
		;;
	--binary)
		next_value "$#" "$1"
		binary_source=$2
		shift
		;;
	--no-start)
		start_service=false
		;;
	--no-egress-check)
		egress_check=false
		;;
	--no-verify)
		verify=false
		;;
	--dry-run)
		dry_run=true
		;;
	-h | --help)
		usage
		exit 0
		;;
	-*)
		usage_error "unknown argument: $1"
		;;
	*)
		record_invite "$1"
		;;
	esac
	shift
done

case $(uname -s) in
Darwin) platform=macos ;;
Linux) platform=linux ;;
*) die "the client installer supports macOS and Linux; use the manual steps in docs/DEPLOYING.md" ;;
esac

if [ -z "$config_dir" ]; then
	if [ "$platform" = macos ]; then
		config_dir="$home/Library/Application Support/queqiao"
	else
		config_dir="${XDG_CONFIG_HOME:-$home/.config}/queqiao"
	fi
fi

if [ "$(id -u)" -eq 0 ]; then
	die "run this as the account that will use the tunnel, not with sudo.
The profile is that user's private key and the service is a per-user agent."
fi

case $base_port in
'' | *[!0-9]*) usage_error "--base-port must be numeric" ;;
esac
if [ "$base_port" -lt 1 ] || [ "$base_port" -gt 65535 ]; then
	usage_error "--base-port $base_port is out of range"
fi

# Paths are emitted verbatim into JSON and, on macOS, into plist XML. Rather
# than escaping every metacharacter in two syntaxes, accept only characters
# that need no escaping in either.
for path_value in "$prefix" "$config_dir"; do
	case $path_value in
	*[!A-Za-z0-9/._@+:~\ -]*)
		die "path contains a character this installer will not quote safely: $path_value"
		;;
	esac
done

binary_path=$prefix/bin/queqiaod
manifest=$config_dir/providers.json
if [ "$platform" = macos ]; then
	service_dir=$home/Library/LaunchAgents
	service_path=$service_dir/$label.plist
else
	service_dir=$home/.config/systemd/user
	service_path=$service_dir/$service_name.service
fi
if [ -z "$device_name" ]; then
	device_name=$(hostname 2>/dev/null || echo device)
fi

# The installed service definition is the only durable record of where a
# previous install put its files, so it is what gets read back. Discovery is by
# content rather than by name: an operator who changes --label or
# --service-name would otherwise look like a first install, and a first install
# does not migrate anything -- it would leave the enrolled profiles behind,
# unreachable and impossible to re-enroll, because invitations are single-use.
previous_definition=
previous_manifest=
previous_binary=
stale_binary=

read_service_arguments() {
	if [ "$platform" = macos ]; then
		sed -n '/<key>ProgramArguments<\/key>/,/<\/array>/p' "$1" |
			sed -n 's|^[[:space:]]*<string>\(.*\)</string>[[:space:]]*$|\1|p'
	else
		# Every token is quoted when written, so quoted-token extraction
		# survives a path containing spaces where field splitting would not.
		sed -n 's/^ExecStart=//p' "$1" | grep -o '"[^"]*"' | sed -e 's/^"//' -e 's/"$//'
	fi
}

find_previous_install() {
	[ -d "$service_dir" ] || return 0
	if [ "$platform" = macos ]; then
		set -- "$service_dir"/*.plist
	else
		set -- "$service_dir"/*.service
	fi
	matches=$work_dir/matches
	: >"$matches"
	for candidate in "$@"; do
		[ -f "$candidate" ] || continue
		grep -q -- '--providers' "$candidate" || continue
		grep -q 'queqiaod' "$candidate" || continue
		printf '%s\n' "$candidate" >>"$matches"
	done
	found=$(wc -l <"$matches" | tr -d ' ')
	[ "$found" -gt 0 ] || return 0
	if [ "$found" -gt 1 ]; then
		die "$service_dir holds more than one queqiao client service:
$(cat "$matches")
Remove the ones you do not want before relocating; this script will not guess
which install is current."
	fi
	previous_definition=$(cat "$matches")
	read_service_arguments "$previous_definition" >"$work_dir/previous-args"
	previous_binary=$(head -n 1 "$work_dir/previous-args")
	previous_manifest=$(awk 'prev == "--providers" { print; exit } { prev = $0 }' \
		"$work_dir/previous-args")
}

stop_previous_service() {
	previous_id=$(basename "$previous_definition")
	if [ "$platform" = macos ]; then
		launchctl bootout "gui/$(id -u)/${previous_id%.plist}" >/dev/null 2>&1 || true
	else
		systemctl --user disable --now "${previous_id%.service}" >/dev/null 2>&1 || true
	fi
}

# The old binary is removed only once nothing else points at it. A second
# install sharing one binary is unusual but cheap to check, and deleting a
# running service's executable would be a poor trade for tidiness.
remove_unreferenced_binary() {
	[ -f "$1" ] || return 0
	if [ -d "$service_dir" ] && grep -rlF -- "$1" "$service_dir" 2>/dev/null | grep -q .; then
		echo "Left $1 in place; another service definition still references it." >&2
		return 0
	fi
	rm -f "$1"
	rmdir "$(dirname "$1")" 2>/dev/null || true
	rmdir "$(dirname "$(dirname "$1")")" 2>/dev/null || true
	echo "Removed the superseded binary $1."
}

name_taken() {
	cut -f1 "$entries" | grep -Fqx "$1"
}

port_taken() {
	cut -f3 "$entries" | grep -Fqx "127.0.0.1:$1"
}

allocate_port() {
	candidate=$base_port
	while port_taken "$candidate"; do
		candidate=$((candidate + 1))
		[ "$candidate" -le 65535 ] || die "no free loopback port at or above $base_port"
	done
	printf '%s' "$candidate"
}

slugify() {
	printf '%s' "$1" | tr '[:upper:]' '[:lower:]' |
		sed -e 's/[^a-z0-9]\{1,\}/-/g' -e 's/^-*//' -e 's/-*$//' -e 's/^\(.\{1,40\}\).*$/\1/'
}

# The manifest is rewritten from this script's own one-object-per-line shape.
# A hand-edited file is not re-serialized blindly: an unrecognized layout stops
# the install rather than dropping providers the operator added by hand.
read_manifest_entries() {
	[ -f "$1" ] || return 0
	declared=$(grep -c '"name"' "$1" || true)
	parsed=$(grep -o '{"name": "[^"]*", "profile": "[^"]*", "listen": "[^"]*"}' "$1" || true)
	found=0
	if [ -n "$parsed" ]; then
		found=$(printf '%s\n' "$parsed" | wc -l | tr -d ' ')
	fi
	if [ "$declared" -ne "$found" ]; then
		die "$1 was edited into a shape this script cannot merge.
Add the new provider by hand, or move the file aside and re-enroll every
provider in one run."
	fi
	[ "$found" -gt 0 ] || return 0
	printf '%s\n' "$parsed" | while IFS= read -r object; do
		rest=${object#\{\"name\": \"}
		entry_name=${rest%%\"*}
		rest=${rest#*\", \"profile\": \"}
		entry_profile=${rest%%\"*}
		rest=${rest#*\", \"listen\": \"}
		entry_listen=${rest%%\"*}
		printf '%s\t%s\t%s\n' "$entry_name" "$entry_profile" "$entry_listen" >>"$entries"
	done
	echo "Keeping $found provider(s) already in $1."
}

# Relocation moves the profiles rather than re-enrolling them. A profile is a
# device private key whose invitation has already been consumed, so moving the
# file is the only way to keep the device; re-enrolling is not available.
relocate_profiles() {
	moved=$work_dir/moved
	: >"$moved"
	while IFS="$tab" read -r entry_name entry_profile entry_listen; do
		destination=$config_dir/$(basename "$entry_profile")
		if [ "$entry_profile" != "$destination" ]; then
			if [ -e "$destination" ]; then
				die "cannot move $entry_profile to $destination: a file is already there.
Move it aside and re-run, or keep the previous --config-dir."
			fi
			if [ ! -f "$entry_profile" ]; then
				die "provider $entry_name refers to $entry_profile, which is missing.
Its invitation is already consumed, so it cannot be re-enrolled; restore that
file from backup before relocating."
			fi
			mv "$entry_profile" "$destination"
			rm -f "$entry_profile.lock"
			echo "Moved $entry_name to $destination."
		fi
		printf '%s\t%s\t%s\n' "$entry_name" "$destination" "$entry_listen" >>"$moved"
	done <"$entries"
	mv "$moved" "$entries"
}

find_previous_install

relocating=false
if [ -n "$previous_definition" ]; then
	if [ "$previous_manifest" != "$manifest" ] ||
		[ "$previous_definition" != "$service_path" ] ||
		[ "$previous_binary" != "$binary_path" ]; then
		relocating=true
	fi
fi

if [ "$relocating" = false ] && [ ! -s "$pending" ] && [ ! -f "$manifest" ]; then
	usage_error "at least one --invite is required for a first install"
fi

if [ "$dry_run" = true ]; then
	if [ "$relocating" = true ]; then
		echo "Would relocate the install described by $previous_definition:"
		[ "$previous_binary" = "$binary_path" ] ||
			echo "  binary    $previous_binary -> $binary_path"
		[ "$previous_manifest" = "$manifest" ] ||
			echo "  manifest  $previous_manifest -> $manifest"
		[ "$previous_definition" = "$service_path" ] ||
			echo "  service   $previous_definition -> $service_path"
		echo "Would stop the running service and move every enrolled profile with it."
	fi
	echo "Would install $binary_path and write $manifest."
	echo "Would enroll $(wc -l <"$pending" | tr -d ' ') invitation(s) with --local-address $local_address as device \"$device_name\"."
	echo "Would allocate loopback SOCKS5 ports from $base_port upward."
	if [ -n "$path_profile" ]; then
		echo "Would run the service with --path-profile $path_profile."
	fi
	if [ "$platform" = macos ]; then
		echo "Would install the LaunchAgent $service_path."
	else
		echo "Would install the systemd user unit $service_path."
	fi
	exit 0
fi

if [ -n "$binary_source" ]; then
	[ -x "$binary_source" ] || die "--binary $binary_source is not an executable file"
	staged_binary=$binary_source
	binary_origin="supplied binary"
elif [ -x "$repo_root/queqiaod" ]; then
	staged_binary=$repo_root/queqiaod
	binary_origin="prebuilt $repo_root/queqiaod"
elif command -v go >/dev/null 2>&1; then
	echo "Building queqiaod from $repo_root ..."
	(cd "$repo_root" && go build -o "$work_dir/queqiaod" ./cmd/queqiaod) ||
		die "go build failed; build it separately and pass --binary PATH"
	staged_binary=$work_dir/queqiaod
	binary_origin="source build"
else
	die "no queqiaod binary found.
Pass --binary PATH, place a built binary at $repo_root/queqiaod, or install Go
and re-run: go build -o ./queqiaod ./cmd/queqiaod"
fi

version_line=$("$staged_binary" version) ||
	die "$staged_binary did not run on this host; check the build architecture"
case $version_line in
*wire=1*) ;;
*) die "refusing to install a binary that does not speak protocol 1: $version_line" ;;
esac
echo "Installing $version_line ($binary_origin)."

install -d -m 0755 "$prefix/bin"
install -d -m 0700 "$config_dir"

# Rename into place so a running client keeps its mapped image.
install -m 0755 "$staged_binary" "$binary_path.new"
mv -f "$binary_path.new" "$binary_path"

if [ "$relocating" = true ]; then
	echo "Relocating the install described by $previous_definition."
	stop_previous_service
	read_manifest_entries "$previous_manifest"
	relocate_profiles
	if [ "$previous_manifest" != "$manifest" ]; then
		rm -f "$previous_manifest"
	fi
	if [ "$previous_definition" != "$service_path" ]; then
		rm -f "$previous_definition"
		echo "Removed the superseded service definition $previous_definition."
	fi
	if [ "$previous_binary" != "$binary_path" ]; then
		stale_binary=$previous_binary
	fi
else
	read_manifest_entries "$manifest"
fi

if [ ! -s "$pending" ] && [ ! -s "$entries" ]; then
	usage_error "at least one --invite is required for a first install"
fi

index=0
while IFS="$tab" read -r invitation requested_name; do
	index=$((index + 1))
	entry_name=
	if [ -n "$requested_name" ]; then
		entry_name=$(slugify "$requested_name")
		[ -n "$entry_name" ] || die "provider name $requested_name has no usable characters"
	fi

	if [ -n "$entry_name" ]; then
		! name_taken "$entry_name" || die "provider $entry_name is already in $manifest"
		profile_path=$config_dir/$entry_name.json
	else
		profile_path=$config_dir/provider-$index.json
	fi
	[ ! -e "$profile_path" ] ||
		die "$profile_path already exists; give this invitation a distinct name with --invite NAME=URI"

	echo "Enrolling into $profile_path ..."
	enroll_output=$("$binary_path" enroll "$invitation" \
		--profile "$profile_path" \
		--device-name "$device_name" \
		--local-address "$local_address") || die "enrollment failed.
A one-time invitation is consumed on use; ask the provider for a new one unless
$profile_path.enrolling was left behind, which the same URI can safely retry.
When two physical uplinks are active, pass --local-address if:NAME."
	echo "$enroll_output"

	# Prefer the provider's own display name over provider-N once enrollment
	# has revealed it. The manifest name tags every runtime log record, so it
	# is worth making it recognizable.
	if [ -z "$entry_name" ]; then
		display_name=$(printf '%s\n' "$enroll_output" |
			sed -n 's/^Enrolled "\(.*\)" as device .*$/\1/p')
		candidate_name=$(slugify "$display_name")
		if [ -n "$candidate_name" ] && ! name_taken "$candidate_name" &&
			[ ! -e "$config_dir/$candidate_name.json" ]; then
			mv "$profile_path" "$config_dir/$candidate_name.json"
			rm -f "$profile_path.lock"
			profile_path=$config_dir/$candidate_name.json
			entry_name=$candidate_name
		else
			entry_name=provider-$index
		fi
	fi

	printf '%s\t%s\t127.0.0.1:%s\n' "$entry_name" "$profile_path" "$(allocate_port)" >>"$entries"
done <"$pending"

[ -s "$entries" ] || die "no providers to configure"

total=$(wc -l <"$entries" | tr -d ' ')
manifest_tmp=$work_dir/providers.json
{
	echo '{'
	echo '  "version": 1,'
	echo '  "providers": ['
	written=0
	while IFS="$tab" read -r entry_name entry_profile entry_listen; do
		written=$((written + 1))
		if [ "$written" -lt "$total" ]; then
			terminator=,
		else
			terminator=
		fi
		printf '    {"name": "%s", "profile": "%s", "listen": "%s"}%s\n' \
			"$entry_name" "$entry_profile" "$entry_listen" "$terminator"
	done <"$entries"
	echo '  ]'
	echo '}'
} >"$manifest_tmp"
install -m 0600 "$manifest_tmp" "$manifest"

# The service definition has one renderer, and it lives in the binary. This
# script owns enrollment, the manifest, and verification; `queqiaod service`
# owns what a LaunchAgent and a systemd user unit look like. Two copies of that
# knowledge is exactly how the old hand-edited plist drifted from the guide.
# Positional parameters rather than one string: the default configuration
# directory on macOS is "Application Support", and word splitting would tear
# that path in half.
set -- service install \
	--providers "$manifest" \
	--local-address "$local_address" \
	--log-level "$log_level" \
	--metrics-listen "$metrics_listen" \
	--label "$label" \
	--service-name "$service_name"
if [ -n "$path_profile" ]; then
	set -- "$@" --path-profile "$path_profile"
fi
if [ "$start_service" = false ]; then
	set -- "$@" --no-start
fi
"$binary_path" "$@"

# Only now is the superseded binary genuinely unreferenced. Until the new
# definition was written, the scan would still have found the old path inside
# it and declined to remove anything.
if [ -n "$stale_binary" ]; then
	remove_unreferenced_binary "$stale_binary"
fi

have_nc=false
if command -v nc >/dev/null 2>&1; then
	have_nc=true
fi

if [ "$verify" = true ] && [ "$start_service" = true ]; then
	if [ "$have_nc" = false ]; then
		echo "NOTE: nc is unavailable; listener verification was skipped." >&2
	fi
	while IFS="$tab" read -r entry_name entry_profile entry_listen; do
		[ "$have_nc" = true ] || continue
		attempt=0
		listening=false
		while [ "$attempt" -lt 15 ]; do
			if nc -z "${entry_listen%:*}" "${entry_listen##*:}" >/dev/null 2>&1; then
				listening=true
				break
			fi
			sleep 1
			attempt=$((attempt + 1))
		done
		if [ "$listening" = false ]; then
			die "$entry_name never accepted a connection on $entry_listen.
Inspect the client log: $binary_path logs client"
		fi
		echo "$entry_name is listening on $entry_listen."

		if [ "$egress_check" = true ] && command -v curl >/dev/null 2>&1; then
			# NO_PROXY=* otherwise bypasses even an explicit --socks5-hostname
			# and produces a convincing but irrelevant result.
			echo "Checking egress for $entry_name through https://api.ipify.org ..."
			egress=$(env -u NO_PROXY -u no_proxy curl --noproxy '' -fsS --max-time 20 \
				--socks5-hostname "$entry_listen" https://api.ipify.org) ||
				die "$entry_name accepted the SOCKS connection but the request did not complete.
Skip this check with --no-egress-check, then read $binary_path logs client."
			echo "$entry_name egress address: $egress"
		fi
	done <"$entries"
fi

cat <<EOF

Queqiao client installed.
  binary    $binary_path
  manifest  $manifest
  service   $service_path
  logs      $binary_path logs client

Providers:
EOF
while IFS="$tab" read -r entry_name entry_profile entry_listen; do
	printf '  %-24s socks5h://%s\n' "$entry_name" "$entry_listen"
done <"$entries"

cat <<EOF

Point Clash or mihomo at the listeners above; deploy/clash-queqiao.yaml is a
starter profile. Add another provider later with:
  deploy/install-client.sh --invite 'queqiao://enroll/...'
EOF
