# Contributing to Queqiao

Queqiao is a ready-to-use, open-source transport that keeps evolving from
evidence on real networks. Code matters, but so do documentation, deployment
experience, measurements, and reports that show where the current design is
wrong.

The project has a precise optimization unit: a known client and gateway whose
shared WAN segment is the dominant bottleneck. Before proposing a large feature,
explain the problem, the supported topology it affects, and how you will
evaluate it. Do not justify protocol or congestion-control changes only with an
emulated benchmark.

## Ways to help

- **Use it.** Deploy the paired client and gateway, try TCP and UDP workloads,
  and report setup friction or failures.
- **Measure it.** Compare short-lived, interactive, and bulk behavior on the
  same path window; [the network-evidence guide](docs/CONTRIBUTING-NETWORK-EVIDENCE.md)
  explains how to make the result useful and safe.
- **Improve the project.** Fix bugs, add tests, improve deployment examples,
  update the mobile clients, or make the local tools easier to use.
- **Change the protocol carefully.** Include a measured problem, a compatibility
  story, resource bounds, and an evaluation plan. Wire changes must be explicit
  and fail closed.

## Development checks

Use Go 1.26.9 or newer in the 1.26 line, then run:

```sh
go test -short -timeout 20m ./...
go vet ./...
test -z "$(gofmt -l .)"
python3 -m unittest discover -s scripts -p 'test_*.py'
./scripts/changelog.py check
go run honnef.co/go/tools/cmd/staticcheck@v0.7.0 -checks=all,-U1000 ./...
```

Changes to framing, authentication, resource admission, recovery,
concurrency, or packaging should also run the relevant full, race, fuzz, and
fallback checks from `.github/workflows/deep.yml`. Normal CI runs the short Go
suite on Linux, macOS, and Windows for amd64 and arm64; deep and release
candidate runs exercise the broader supported matrix.

A wire change must increment `internal/protocol.Version`, update
[`docs/PROTOCOL.md`](docs/PROTOCOL.md), document its upgrade path, and add
fail-closed compatibility tests. Keep the [current design](docs/DESIGN.md),
[architecture](docs/ARCHITECTURE.md), and [known limitations](docs/KNOWN-LIMITATIONS.md)
consistent with any guarantee that changes.

Measurements must record the commit, toolchain, module graph, exact command,
path parameters, trial order, and raw output. Alternate candidates or use a
shared path window; do not run transports in separate time windows and
attribute route movement to code.

The full gosec invocation intentionally reports reviewed findings. Validate its
JSON with `scripts/check_gosec.py` as documented in the archived static-security
audit; do not silence a rule globally. A release candidate requires fresh
security and dependency evidence for its exact commit.

## Pull requests

Keep changes scoped and explain how they were checked. Preserve failed and
incomplete measurements rather than silently dropping them. Never commit
credentials, private keys, active host configuration, packet captures with user
traffic, or reports containing private infrastructure details.

### Changelog entries

Do not edit `CHANGELOG.md`. It is assembled at release time, and a branch that
writes to it conflicts with every other branch that also did. A user-visible
change adds one file to [`changelog.d/`](changelog.d/) instead:

```sh
./scripts/changelog.py new fixed provider-unit-bind-capability
./scripts/changelog.py preview   # what the next release will say
```

The file is named `<slug>.<category>.md` for one of `added`, `changed`,
`deprecated`, `removed`, `fixed`, or `security`, and holds the entry as prose
wrapped at 78 columns with no leading `- `. Two pull requests never write the
same file, so there is nothing to resolve when both merge. A change with no
user-visible effect — a refactor, a test, internal documentation — needs no
file. CI checks the pending files and rejects a branch that edits
`CHANGELOG.md`. Cutting a release, or deliberately correcting text that already
shipped, is what the `changelog` label on a pull request is for; add the label
and re-run the `changelog` job, which reads it at the moment it runs.

This applies to generated and agent-authored branches too. Nothing in this
repository writes `CHANGELOG.md` except `./scripts/changelog.py release`, and a
released section is history: appending to it after its version has shipped is
the mistake this layout exists to prevent.

For security vulnerabilities, follow [`SECURITY.md`](SECURITY.md) and report
privately instead of opening a public issue or pull request.
