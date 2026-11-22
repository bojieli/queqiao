#!/bin/sh
set -eu

# One-command Queqiao provider install for a Linux systemd host. It performs
# every step of the gateway section of docs/DEPLOYING.md: binary, service
# account, directories, provider trust root, first user, environment file,
# hardened unit, and post-start verification. The final invitation is issued
# only after the running gateway has been verified.
#
# Re-running the script over an initialized provider is refused by default. A
# trust root cannot be replaced without stranding every enrolled device, so an
# upgrade must say so explicitly with --no-provider-init.

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH='' cd -- "$script_dir/.." && pwd)

prefix=/usr/local
config_dir=/etc/queqiao
log_dir=/var/log/queqiao
state=/var/lib/queqiao/provider
run_user=queqiao
service_name=queqiaod
backup_root=/var/lib/queqiao-rollback

binary_source=
provider_name=
endpoint=
listen=
user_name=
user_max_flows=1024
user_max_clients=8
invite_expires_in=24h
transport=auto
max_sessions=4096
metrics_listen=127.0.0.1:19090
extra_args=
bootstrap=true
tune=false
verify=true
dry_run=false

staged_binary=
build_dir=
backed_up=false

usage() {
	cat <<'EOF'
Usage: deploy/install-server.sh --name NAME --endpoint HOST:PORT --user USER [options]

Install queqiaod as a systemd gateway service, initialize a provider, create
the first user, and print one single-use invitation URI.

Required for a first install:
  --name NAME              provider display name shown to users
  --endpoint HOST:PORT     public gateway address placed in every invitation
  --user USER              first user account to create

Provider options:
  --state DIR              provider state directory (default /var/lib/queqiao/provider)
  --user-max-clients N     per-user concurrent device limit (default 8, 0 for every device)
  --user-max-flows N       per-user concurrent flow limit (default 1024, 0 for the
                           gateway-wide limit). One flow is one TCP connection or
                           one UDP association, so a browser needs hundreds: this
                           is not a device count.
  --invite-expires-in DUR  invitation lifetime, maximum 7d (default 24h)
  --no-provider-init       upgrade an existing deployment; skip init/add-user/invite

Service options:
  --listen ADDR            server listen address (default :PORT from --endpoint)
  --transport auto|quic|tcp  (default auto)
  --max-sessions N         gateway-wide concurrent flow limit (default 4096)
  --metrics-listen ADDR    loopback metrics address (default 127.0.0.1:19090, "" disables)
  --extra-args "ARGS"      additional queqiaod server flags
  --service-name NAME      systemd unit name without .service (default queqiaod)
  --run-user NAME          service account (default queqiao)
  --prefix DIR             install prefix (default /usr/local)
  --config-dir DIR         environment file directory (default /etc/queqiao)
  --log-dir DIR            runtime log directory (default /var/log/queqiao)

Other options:
  --binary PATH            install this queqiaod instead of building one
  --tune                   also run deploy/tune-server.sh for socket limits
  --no-verify              skip the post-start listener and metrics checks
  --dry-run                print the plan and exit without changing the host
  -h, --help               show this help

The binary is taken from --binary, then ./queqiaod in the repository root,
then a local `go build ./cmd/queqiaod`.
EOF
}

die() {
	echo "install-server.sh: $*" >&2
	exit 1
}

usage_error() {
	echo "install-server.sh: $*" >&2
	echo "Run deploy/install-server.sh --help for usage." >&2
	exit 2
}

next_value() {
	if [ "$1" -lt 2 ]; then
		usage_error "$2 requires a value"
	fi
}

reject_whitespace() {
	case $2 in
	*[[:space:]]*)
		die "$1 must not contain whitespace: $2"
		;;
	esac
}

while [ "$#" -gt 0 ]; do
	case $1 in
	--name)
		next_value "$#" "$1"
		provider_name=$2
		shift
		;;
	--endpoint)
		next_value "$#" "$1"
		endpoint=$2
		shift
		;;
	--user)
		next_value "$#" "$1"
		user_name=$2
		shift
		;;
	--state)
		next_value "$#" "$1"
		state=$2
		shift
		;;
	--user-max-clients)
		next_value "$#" "$1"
		user_max_clients=$2
		shift
		;;
	--user-max-flows)
		next_value "$#" "$1"
		user_max_flows=$2
		shift
		;;
	--user-max-sessions)
		next_value "$#" "$1"
		printf '%s\n' "install-server.sh: --user-max-sessions is the former name of --user-max-flows; use --user-max-clients to limit devices." >&2
		user_max_flows=$2
		shift
		;;
	--invite-expires-in)
		next_value "$#" "$1"
		invite_expires_in=$2
		shift
		;;
	--no-provider-init)
		bootstrap=false
		;;
	--listen)
		next_value "$#" "$1"
		listen=$2
		shift
		;;
	--transport)
		next_value "$#" "$1"
		transport=$2
		shift
		;;
	--max-sessions)
		next_value "$#" "$1"
		max_sessions=$2
		shift
		;;
	--metrics-listen)
		next_value "$#" "$1"
		metrics_listen=$2
		shift
		;;
	--extra-args)
		next_value "$#" "$1"
		extra_args=$2
		shift
		;;
	--service-name)
		next_value "$#" "$1"
		service_name=$2
		shift
		;;
	--run-user)
		next_value "$#" "$1"
		run_user=$2
		shift
		;;
	--prefix)
		next_value "$#" "$1"
		prefix=$2
		shift
		;;
	--config-dir)
		next_value "$#" "$1"
		config_dir=$2
		shift
		;;
	--log-dir)
		next_value "$#" "$1"
		log_dir=$2
		shift
		;;
	--binary)
		next_value "$#" "$1"
		binary_source=$2
		shift
		;;
	--tune)
		tune=true
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
	*)
		usage_error "unknown argument: $1"
		;;
	esac
	shift
done

if [ "$bootstrap" = true ]; then
	[ -n "$provider_name" ] || usage_error "--name is required (or use --no-provider-init)"
	[ -n "$endpoint" ] || usage_error "--endpoint is required (or use --no-provider-init)"
	[ -n "$user_name" ] || usage_error "--user is required (or use --no-provider-init)"
fi

# The environment file is a whitespace-separated argument list, so a path
# containing whitespace would silently split into two arguments at start-up.
reject_whitespace --state "$state"
reject_whitespace --prefix "$prefix"
reject_whitespace --config-dir "$config_dir"
reject_whitespace --log-dir "$log_dir"
reject_whitespace --metrics-listen "$metrics_listen"

case $state in
/*) ;;
*) die "--state must be an absolute path" ;;
esac

if [ -n "$endpoint" ]; then
	case $endpoint in
	*:*) ;;
	*) usage_error "--endpoint must be HOST:PORT; every invitation carries it verbatim" ;;
	esac
	case ${endpoint##*:} in
	'' | *[!0-9]*) usage_error "--endpoint must end in a numeric port: $endpoint" ;;
	esac
fi

if [ -z "$listen" ]; then
	if [ -z "$endpoint" ]; then
		usage_error "--listen is required when --endpoint is not given"
	fi
	listen=":${endpoint##*:}"
fi
listen_port=${listen##*:}
case $listen_port in
'' | *[!0-9]*)
	die "could not read a numeric port from --listen $listen"
	;;
esac
if [ "$listen_port" -lt 1 ] || [ "$listen_port" -gt 65535 ]; then
	die "--listen port $listen_port is out of range"
fi

state_parent=$(dirname -- "$state")
unit_template=$script_dir/queqiaod.service
unit_path=/etc/systemd/system/$service_name.service
env_path=$config_dir/queqiaod.env
binary_path=$prefix/bin/queqiaod

server_args="--state $state --listen $listen --transport $transport --max-sessions $max_sessions"
if [ -n "$metrics_listen" ]; then
	server_args="$server_args --metrics-listen $metrics_listen"
fi
if [ -n "$extra_args" ]; then
	server_args="$server_args $extra_args"
fi

if [ "$dry_run" = true ]; then
	cat <<EOF
Would install:
  binary          $binary_path
  unit            $unit_path
  environment     $env_path
  service account $run_user
  state           $state (mode 0700)
  logs            $log_dir (mode 0750)

Would write QUEQIAOD_ARGS=$server_args
EOF
	if [ "$bootstrap" = true ]; then
		cat <<EOF

Would initialize provider "$provider_name" at $endpoint, create user
"$user_name" with --max-clients $user_max_clients and --max-flows
$user_max_flows, and print one invitation valid for $invite_expires_in.
EOF
	else
		echo
		echo "Would reuse the existing provider state at $state."
	fi
	if [ "$tune" = true ]; then
		echo "Would run deploy/tune-server.sh --service $service_name.service."
	fi
	exit 0
fi

if [ "$(id -u)" -ne 0 ]; then
	die "this script must run as root; use: sudo $0 ..."
fi
if [ "$(uname -s)" != Linux ]; then
	die "the provider service installer supports Linux systemd hosts only"
fi
command -v systemctl >/dev/null 2>&1 || die "systemctl is required"
[ -f "$unit_template" ] || die "missing unit template $unit_template"
if [ "$tune" = true ] && [ ! -x "$script_dir/tune-server.sh" ]; then
	die "--tune needs $script_dir/tune-server.sh, which is missing or not executable"
fi

# The refusal below is the whole safety property of this script. Replacing a
# provider root invalidates every issued device certificate, so an existing
# state directory stops the install before anything on the host is touched.
if [ "$bootstrap" = true ] && [ -e "$state" ]; then
	die "provider state already exists at $state.
Re-run with --no-provider-init to upgrade the binary, unit, and environment
file while keeping this trust root. Never run provider init over or beside an
existing root: it would strand every enrolled device."
fi
if [ "$bootstrap" = false ] && [ ! -d "$state" ]; then
	die "--no-provider-init was given but $state does not exist; the service would fail to start"
fi

cleanup() {
	if [ -n "$build_dir" ]; then
		rm -rf "$build_dir"
	fi
}
trap cleanup 0 HUP INT TERM

if [ -n "$binary_source" ]; then
	[ -x "$binary_source" ] || die "--binary $binary_source is not an executable file"
	staged_binary=$binary_source
	binary_origin="supplied binary"
elif [ -x "$repo_root/queqiaod" ]; then
	staged_binary=$repo_root/queqiaod
	binary_origin="prebuilt $repo_root/queqiaod"
elif command -v go >/dev/null 2>&1; then
	echo "Building queqiaod from $repo_root ..."
	build_dir=$(mktemp -d)
	(cd "$repo_root" && go build -o "$build_dir/queqiaod" ./cmd/queqiaod) ||
		die "go build failed; build it separately and pass --binary PATH"
	staged_binary=$build_dir/queqiaod
	binary_origin="source build"
else
	die "no queqiaod binary found.
Pass --binary PATH, place a built binary at $repo_root/queqiaod, or install Go
and re-run: go build -o ./queqiaod ./cmd/queqiaod"
fi

version_line=$("$staged_binary" version) ||
	die "$staged_binary did not run on this host; check the build architecture"
case " $version_line " in
*" wire=2 "*) ;;
*)
	die "refusing to install a binary that does not speak protocol 2: $version_line"
	;;
esac
echo "Installing $version_line ($binary_origin)."

if ! id -u "$run_user" >/dev/null 2>&1; then
	command -v useradd >/dev/null 2>&1 ||
		die "user $run_user does not exist and useradd is unavailable; create it manually"
	useradd --system --user-group \
		--home-dir "$state_parent" --shell /usr/sbin/nologin "$run_user"
	echo "Created service account $run_user."
fi

install -d -m 0755 -o root -g root "$config_dir"
install -d -m 0750 -o "$run_user" -g "$run_user" "$log_dir"

# Create the state parent only when it is missing. Silently applying 0700 and
# a new owner to a directory the operator already uses for something else --
# /srv, /opt, a mount point -- would be a far worse surprise than leaving a
# mode alone. The gateway itself refuses a group- or world-accessible provider
# directory at start-up, which is the check that actually protects the keys.
if [ ! -d "$state_parent" ]; then
	install -d -m 0700 -o "$run_user" -g "$run_user" "$state_parent"
else
	echo "NOTE: $state_parent already exists; its mode and owner were left unchanged." >&2
fi

stamp=$(date -u +%Y%m%dT%H%M%SZ)
backup_dir=$backup_root/$stamp
save_rollback_copy() {
	[ -e "$1" ] || return 0
	install -d -m 0700 -o root -g root "$backup_dir"
	cp -p "$1" "$backup_dir/"
	backed_up=true
}

save_rollback_copy "$binary_path"
save_rollback_copy "$unit_path"
save_rollback_copy "$env_path"

# Rename into place so an already running gateway keeps its mapped image and
# the new file appears atomically rather than being written through.
install -m 0755 -o root -g root "$staged_binary" "$binary_path.new"
mv -f "$binary_path.new" "$binary_path"

if [ "$bootstrap" = true ]; then
	provider_run() {
		if command -v runuser >/dev/null 2>&1; then
			runuser -u "$run_user" -- "$@"
		elif command -v sudo >/dev/null 2>&1; then
			sudo -u "$run_user" -- "$@"
		else
			die "runuser or sudo is required to run provider commands as $run_user"
		fi
	}

	provider_run "$binary_path" provider init \
		--state "$state" --name "$provider_name" --endpoint "$endpoint"
	provider_run "$binary_path" provider add-user \
		--state "$state" --name "$user_name" \
		--max-flows "$user_max_flows" --max-clients "$user_max_clients"
	echo "Created user $user_name."
fi

umask 077
env_tmp=$(mktemp "$env_path.tmp.XXXXXX")
printf 'QUEQIAOD_ARGS=%s\n' "$server_args" >"$env_tmp"
install -o root -g "$run_user" -m 0640 "$env_tmp" "$env_path"
rm -f "$env_tmp"

unit_tmp=$(mktemp "$unit_path.tmp.XXXXXX")
sed \
	-e "s|^User=.*|User=$run_user|" \
	-e "s|^Group=.*|Group=$run_user|" \
	-e "s|^EnvironmentFile=.*|EnvironmentFile=$env_path|" \
	-e "s|^Environment=QUEQIAO_LOG_DIR=.*|Environment=QUEQIAO_LOG_DIR=$log_dir|" \
	-e "s|^ExecStart=.*|ExecStart=$binary_path server \$QUEQIAOD_ARGS|" \
	-e "s|^ReadWritePaths=.*|ReadWritePaths=$state_parent $log_dir|" \
	"$unit_template" >"$unit_tmp"
install -o root -g root -m 0644 "$unit_tmp" "$unit_path"
rm -f "$unit_tmp"

systemctl daemon-reload
systemctl enable "$service_name" >/dev/null
systemctl restart "$service_name"

if [ "$tune" = true ]; then
	"$script_dir/tune-server.sh" --service "$service_name.service"
fi

wait_for() {
	wait_attempt=0
	while [ "$wait_attempt" -lt 20 ]; do
		if "$@" >/dev/null 2>&1; then
			return 0
		fi
		sleep 1
		wait_attempt=$((wait_attempt + 1))
	done
	return 1
}

if [ "$verify" = true ]; then
	wait_for systemctl is-active --quiet "$service_name" || {
		systemctl --no-pager --lines=30 status "$service_name" || true
		die "$service_name did not become active; inspect: journalctl -u $service_name -n 50"
	}

	if command -v ss >/dev/null 2>&1; then
		# Type=simple reports active as soon as the process is forked, so the
		# socket is not bound yet on the first look. Checking once turned a
		# successful upgrade into a failed install with a message saying the
		# gateway was not listening while it was already serving traffic.
		port_listening() {
			ss -lntup 2>/dev/null | grep -q ":$listen_port "
		}
		wait_for port_listening ||
			die "nothing is listening on port $listen_port after start-up"
		listeners=$(ss -lntup 2>/dev/null | grep ":$listen_port " || true)
		echo "$listeners"
		if [ "$transport" = auto ]; then
			echo "$listeners" | grep -q '^udp' ||
				echo "WARNING: no UDP listener on port $listen_port; QUIC will not be reachable." >&2
			echo "$listeners" | grep -q '^tcp' ||
				echo "WARNING: no TCP listener on port $listen_port; the fallback transport is unavailable." >&2
		fi
	else
		echo "NOTE: ss is unavailable; the listener check was skipped." >&2
	fi

	if [ -n "$metrics_listen" ] && command -v curl >/dev/null 2>&1; then
		wait_for curl -fsS "http://$metrics_listen/metrics" ||
			die "metrics endpoint http://$metrics_listen/metrics did not respond"
		echo "Metrics endpoint http://$metrics_listen/metrics is responding."
	fi

	wait_for test -s "$log_dir/server.log" ||
		echo "NOTE: $log_dir/server.log is still empty; run queqiaod logs server to check." >&2
fi

invitation=
if [ "$bootstrap" = true ]; then
	invitation=$(provider_run "$binary_path" provider invite \
		--state "$state" --user "$user_name" --expires-in "$invite_expires_in")
fi

service_state=$(systemctl is-active "$service_name" || true)

cat <<EOF

Queqiao gateway installed.
  service      $service_name ($service_state)
  binary       $binary_path
  environment  $env_path
  state        $state
  logs         $log_dir/server.log
EOF
if [ "$backed_up" = true ]; then
	echo "  rollback     $backup_dir"
fi
if [ "$tune" = false ]; then
	echo
	echo "Next: sudo $script_dir/tune-server.sh   # socket buffers and backlogs"
fi
echo "Open port $listen_port for both TCP and UDP in the host firewall and the cloud security group."

if [ -n "$invitation" ]; then
	cat <<EOF

Send this single-use invitation to $user_name over an authenticated private
channel. It is a bearer credential valid for $invite_expires_in; do not paste it
into a shared log or ticket, and revoke it with 'queqiaod provider
revoke-invite' if it is not used.

$invitation

On the client host: deploy/install-client.sh --invite '$invitation'
EOF
fi
