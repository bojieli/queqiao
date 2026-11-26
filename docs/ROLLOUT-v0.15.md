# Rollout plan: the path-authority defaults (v0.15)

This is the production rollout of stage 10 and stage 11 of
[DESIGN-PATH-AUTHORITY.md](DESIGN-PATH-AUTHORITY.md) (section 14): the
path-authority bundle becomes the default on both roles, soaks, and then the
legacy code is deleted and v0.15.0 is cut. It names hosts by role only. The
mapping from these labels to real hosts, units and paths is kept in a local
file beside this one (`ROLLOUT-v0.15.local.md`, git-ignored), never here.

| Label | Role |
| --- | --- |
| `GW-1`, `GW-2`, `GW-3` | production gateways (`queqiaod server`, systemd `queqiaod.service`) |
| `CLIENT-1` | the desktop client (`queqiaod client`, installed with `queqiaod service install`) |
| `RELAY-1`, `RELAY-2` | relay hosts in front of the gateways (`queqiaod relay run` units) |

Nothing here starts before the stage-10 pull request has passed its gates
(design section 15.3 and 15.4: trials A, B, C and D and the DC gate on every
path, and the deterministic replacements in CI) and has merged. The owner
decides when each phase starts, and whether v0.15.0 is published.

## What changes, and what does not

- **Behaviour.** With no switch flags, both roles run the path-authority
  bundle: `cc-api=v2`, `sched=class`, `congestion=path`,
  `coded-retry=sticky`, `recovery=carrier`, `mem=owned` and
  `classifier=phased`, plus `gateway-admission=owned` on a gateway and
  `topology=single` on a client (`docs/DEPLOYING.md`, "The path-authority
  bundle").
- **Wire.** Unchanged. A gateway still accepts wires 1 and 2 by default, and
  serves clients on wire 1, released clients, and clients on the legacy
  bundle. That is why gateways go first: every client, old or new, keeps
  working against an upgraded gateway, and a new client works against a
  gateway not yet upgraded.
- **Admission.** Under `gateway-admission=owned` the `--max-sessions`
  ceiling counts flows rather than streams. A value sized for streams is
  therefore at least as generous as before; it does not need to change.
- **Relays** forward bytes and run no bundle. They are upgraded only to keep
  one binary version per host.

## The two phases

| Phase | Build | Rollback |
| --- | --- | --- |
| 1. Soak | `main` at the stage-10 merge, built by the release-candidate workflow (no published release) | `--bundle=legacy` on the host, or the previous binary |
| 2. Release | v0.15.0, after stage 11 deletes the legacy code | the previous binary only (the soak build, or v0.14.0) |

Phase 1 is the design's soak: the new defaults run in production while the
legacy bundle is still one flag away. Phase 2 starts only after phase 1 has
run clean on every host for at least seven days, and stage 11 has merged.

## Before anything

On every host, record and keep with the local file:

1. `queqiaod version`, and the SHA-256 of the installed binary.
2. The exact service arguments: `systemctl cat queqiaod.service` (gateways),
   the relay units, and on `CLIENT-1` the LaunchAgent or systemd user unit.
3. A metrics baseline over the previous 24 hours, from each endpoint's
   `--metrics-listen`: `queqiao_flows_completed_total`,
   `queqiao_flows_failed_total`, `queqiao_flow_timeouts_total`,
   `queqiao_lane_join_refused_total` by reason,
   `queqiao_account_admission_refused_total` by reason, and
   `queqiao_data_connections_total` by wire. The failure ratio is
   `failed / (completed + failed)` over a window.
4. A backup of provider state on each gateway (`docs/DEPLOYING.md`, "Backup
   and monitoring") and of the enrolled profile on `CLIENT-1`.
5. Copy the current binary beside itself as `queqiaod.prev` on every host.
   It is the binary rollback for the whole rollout.

## Health checks

Run these after every step, and again at the end of each wait. A step is
healthy when all of them hold.

- **H1, process.** The service is active and has not restarted since the
  step (`systemctl show -p ActiveState,NRestarts,ExecMainStartTimestamp`
  on Linux; `launchctl print gui/$UID/<label>` on macOS).
- **H2, configuration.** The newest `runtime configuration` log record
  shows the intended bundle: `bundle_name` is `path-authority` (or `legacy`
  after a flag rollback) and `bundle` lists the switches, for example
  `cc-api=v2,sched=class,congestion=path,recovery=carrier,coded-retry=sticky,mem=owned,gateway-admission=owned,classifier=phased`
  on a gateway. `queqiaod version` reports `wire=2 wire_accept=1,2`.
- **H3, traffic.** Through `CLIENT-1`, an HTTPS request through each
  provider's SOCKS listener succeeds
  (`curl --socks5-hostname 127.0.0.1:<port> -o /dev/null -w '%{http_code}' https://example.com/`),
  and a UDP DNS query through it is answered. Through each relay, the same
  from a client whose profile points at that relay.
- **H4, failures.** Over the window since the step, the flow failure ratio is
  at most twice the baseline's and at most 2 % absolute; flow timeouts and
  `queqiao_lane_join_refused_total` do not grow faster than in the baseline;
  `queqiao_account_admission_refused_total{reason="flow_limit"}` does not
  appear where it was zero.
- **H5, old peers (gateways).** Connections on wire 1, if the baseline had
  any, still arrive (`queqiao_data_connections_total{wire="1"}` keeps
  rising) and their flows complete.
- **H6, resources.** Gateway CPU and resident memory stay within 1.5x of the
  baseline at similar load; on `CLIENT-1`, the process stays within its usual
  memory.

Any failed check is a stop: do not take the next step, and roll back the
host if the check does not recover within 15 minutes.

## Phase 1: soak the new defaults

Each step installs the soak build the same way:

```sh
# on the host, with the new binary uploaded as /tmp/queqiaod.new
/tmp/queqiaod.new version                 # expect the soak build and wire=2 wire_accept=1,2
sudo install -m 0755 /tmp/queqiaod.new /usr/local/bin/queqiaod.new
sudo mv /usr/local/bin/queqiaod.new /usr/local/bin/queqiaod   # atomic replace
```

No service arguments change: the defaults carry the new bundle. If a unit
names `--congestion` other than `erasure`, or a client's `--quic-pool=false`,
the new build refuses to start and says so. Remove that flag, or add
`--bundle=legacy` to keep that host on the old behaviour, before restarting.

1. **`GW-1`.** Install, `sudo systemctl restart queqiaod`. A restart ends
   the flows the gateway carries, as any restart does; clients reconnect on
   their own. Check H1 to H6 at once, then wait one hour under normal traffic
   and check again.
2. **`GW-2`**, then **`GW-3`**: the same, one at a time, each with its
   one-hour wait.
3. **All gateways soak for 24 hours** with clients unchanged. This is the
   mixed fleet of design section 12: released clients, which isolate and
   rescue per flow, against gateways that attach each device's connections to
   one path controller.
4. **`RELAY-1`**, then **`RELAY-2`**: install, restart each relay unit
   (`sudo systemctl restart <relay unit>`), check H1 and H3 through that relay.
5. **`CLIENT-1`.** Replace the binary the service runs and restart it
   (`launchctl kickstart -k gui/$UID/<label>` on macOS, or
   `systemctl --user restart <unit>` on Linux). Check H1 to H4 and H6 at once,
   after one hour, and after 24 hours.
6. **Soak for at least seven days** with every host on the soak build. Check
   H4 and H6 daily on every host.

### Roll back, phase 1

Roll back the host that failed, not the fleet, unless the failure is on every
host. Prefer the flag rollback, which keeps the build and changes only the
bundle; use the binary rollback when the build itself is at fault (a crash,
or a failure that `--bundle=legacy` does not clear).

**Gateway, by flag.** Add `--bundle=legacy` to the service's command line in
a drop-in, so the unit file itself is untouched:

```sh
sudo systemctl edit queqiaod.service
# In the editor, repeat the current ExecStart (from `systemctl cat`) with the flag:
#   [Service]
#   ExecStart=
#   ExecStart=<the recorded ExecStart, unchanged> --bundle=legacy
sudo systemctl restart queqiaod
```

H2 must then show `bundle_name` `legacy` and `bundle` `legacy`. To undo it,
`sudo systemctl revert queqiaod.service` and restart.

**Gateway, by binary.**

```sh
sudo install -m 0755 /usr/local/bin/queqiaod.prev /usr/local/bin/queqiaod
sudo systemctl revert queqiaod.service   # only if a drop-in was added
sudo systemctl restart queqiaod
```

`queqiaod version` must report the recorded previous version.

**`CLIENT-1`, by flag.** Reinstall the service with the recorded arguments
plus the bundle, which re-renders the unit and restarts it:

```sh
queqiaod service install <the recorded install arguments> --bundle legacy
```

To undo it, run the same command without `--bundle legacy`.

**`CLIENT-1`, by binary.** Copy `queqiaod.prev` over the binary the service
names and restart the service as in step 5.

**Relays, by binary.** Copy `queqiaod.prev` back and restart each relay unit.
Relays have no bundle, so there is no flag rollback.

A host rolled back by flag or binary stays compatible with every other host:
the wire is the same, and a gateway on either bundle serves clients on either
bundle.

## Phase 2: v0.15.0

Stage 11 deletes the legacy bundle with its switches and `--bundle`. Before
installing a build from after stage 11 on any host:

- the host must be healthy on the soak build with no switch flags and no
  `--bundle` in its service arguments, because the new build rejects them; and
- no host may still need the legacy bundle. If one does, phase 2 waits.

Then repeat the phase 1 order and checks: `GW-1`, `GW-2`, `GW-3` (one hour
each), 24 hours on gateways, the relays, `CLIENT-1`.

### Roll back, phase 2

There is no flag rollback. Reinstall the previous binary as in phase 1: the
soak build if it was healthy, otherwise v0.14.0 (`queqiaod.prev`). Either one
interoperates with v0.15.0 on both wires, so a single host can be rolled back
alone. A host rolled back to the soak build runs the path-authority bundle by
default; add `--bundle=legacy` there only if the failure needs the legacy
behaviour.

## Record

For every step, write down in the local file: the time, the build's
`queqiaod version` and SHA-256, the result of each health check, and any
rollback with its reason. After phase 2, the soak result and the release
decision go in the stage-11 pull request.
