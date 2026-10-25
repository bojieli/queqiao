# Releases, installation, and rollback

Release archives are built by `cmd/queqiaopack`, not by host-specific `tar`
or `zip` commands. The packager fixes archive ordering, timestamps, ownership,
permissions, build metadata, and Go path information. Given the same commit,
Go toolchain, version, and commit timestamp, it produces the same bytes.

## Assemble the changelog

`CHANGELOG.md` is written only here. Between releases every user-visible change
lands as its own file in [`changelog.d/`](../changelog.d/), which is why two
branches that both changed behavior never conflicted over this file. Cutting a
release turns those files into one dated section:

```sh
./scripts/changelog.py preview                      # read it before committing
./scripts/changelog.py release --version v0.1.2     # --date defaults to today
```

The command inserts the new section above the newest released one, groups the
entries by category, and deletes every file it consumed; released sections are
left byte for byte alone. It refuses a version already in the file, a version
that is not `vMAJOR.MINOR.PATCH`, an unparsable pending file, and an empty
pending set unless `--allow-empty` says the release really carries no
user-visible change.

Review the assembled section as prose before committing it. Grouping is
mechanical and ordering within a category is by filename, so an entry that
belongs in a different category, or a set of entries that reads better merged
into one, is corrected here — it is the last point at which the wording is
still cheap to change. Commit the result together with the deletions, open it
as a pull request labeled `changelog` (the label is what permits a branch to
touch `CHANGELOG.md`), and tag the merged commit.

## Build a release locally

Start from a clean checkout at the commit being released. Use the commit time,
not the wall clock, as the build date:

```sh
version=v0.1.0
commit=$(git rev-parse HEAD)
build_date=$(git show -s --format=%cI HEAD)
GOENV=off GOEXPERIMENT= GOFIPS140=off GOFLAGS=-mod=readonly \
  GOTOOLCHAIN=go1.26.10 GOWORK=off GOAMD64=v1 GOARM64=v8.0 \
  go run ./cmd/queqiaopack \
  --version "$version" --commit "$commit" --build-date "$build_date" \
  --output dist
```

The default target set is Linux, macOS, and Windows on amd64 and arm64. The
output directory must not already exist, preventing a new release from being
mixed with stale artifacts. Every target has an adjacent CycloneDX SBOM, and
each archive contains the same SBOM plus complete license texts for the exact
modules linked into its binary. `SHA256SUMS` covers every archive and SBOM.
The packager refuses a checkout containing modified, untracked, or ignored
files, a `--commit` other than the full current HEAD SHA, or a `--build-date`
other than that commit's committer timestamp. It requires the exact patched Go
toolchain declared in `go.mod`, verifies the downloaded module cache, and
disables ambient Go workspaces, overlays, build flags, experiments, and
architecture-level changes; the provenance fields therefore cannot silently
describe different source bytes.

Verify a downloaded release before unpacking it:

```sh
sha256sum -c SHA256SUMS                 # Linux
shasum -a 256 -c SHA256SUMS             # macOS
```

Then confirm that the embedded metadata matches the tag:

```sh
./queqiaod --version
cat BUILDINFO
```

Both outputs must report `wire=1` / `wire_protocol=1`. The public protocol-1 contract
accepts only protocol 1 and intentionally provides no compatibility or
downgrade path for the removed shared-secret protocol.

Validate the complete directory before executing an archive:

```sh
./scripts/validate_release.py dist
```

The validator checks the complete six-target matrix and its shared provenance,
checksum coverage, archive paths and modes, build metadata, binary hashes,
internal/external SBOM identity, dependency/license coverage, and the wire
version.

## Atomic Unix installation

Do not overwrite the running binary without retaining the exact prior build.
The running process is unaffected by a rename, so installation and rollback
can be prepared before a controlled service restart:

```sh
sudo install -d -m 0755 /usr/local/lib/queqiao/rollback
installed=/usr/local/bin/queqiaod
backup=/usr/local/lib/queqiao/rollback/queqiaod-$(date -u +%Y%m%dT%H%M%SZ)
test ! -e "$installed" || sudo cp -p "$installed" "$backup"
sudo install -m 0755 ./queqiaod "${installed}.new"
sudo mv "${installed}.new" "$installed"
sudo systemctl restart queqiaod
sudo systemctl is-active --quiet queqiaod
"$installed" --version
```

Keep provider state and client profiles outside the release directory.
Upgrading the binary then cannot replace identities or known working state.

For a macOS LaunchAgent, use the same `install`/`mv` sequence for
`~/.queqiao/bin/queqiaod`, then run:

```sh
launchctl kickstart -k "gui/$(id -u)/me.01.queqiao.client"
```

## Roll back

Choose the backup explicitly, verify its embedded version, install it through
a temporary pathname, and restart:

```sh
backup=/usr/local/lib/queqiao/rollback/queqiaod-YYYYMMDDTHHMMSSZ
"$backup" --version
sudo install -m 0755 "$backup" /usr/local/bin/queqiaod.rollback
sudo mv /usr/local/bin/queqiaod.rollback /usr/local/bin/queqiaod
sudo systemctl restart queqiaod
sudo systemctl is-active --quiet queqiaod
```

If a new build fails its smoke check, roll back the binary before changing the
configuration. That preserves one variable per incident. The Clash side needs
no binary rollback: select the previous profile or remove the `queqiao` SOCKS5
node and rule.

## Non-publishing release candidate

Run `.github/workflows/release-candidate.yml` manually on the exact commit under
review with a version such as `v0.1.0-rc.1`. It runs the full, fuzz,
vulnerability, fallback, history-secret, and reproducibility gates. The
portable Go suite runs natively on Linux, macOS, and Windows for amd64 and
arm64. The high-erasure wall-clock emulator campaigns run on Linux and macOS,
where the configured path timing is preserved; hosted Windows timers can
stretch a configured 300 ms RTT into seconds and therefore do not provide
valid path evidence. Windows still runs coded-path correctness, conformance,
socket-error, and packaged-binary checks. The repository-wide race matrix is
kept in the weekly and on-demand `deep.yml` campaign instead of blocking each
release candidate for hours. Normal CI continues to race-test the mobile core,
and the candidate fallback soak retains its targeted race repetitions. A
separate six-native-runner gate executes each packaged
archive. It also refuses to qualify a commit unless an exact-SHA push or manual
run of the normal CI workflow succeeded, binding the Android, iOS, emulator,
and short native-suite evidence into the candidate decision. The fail-closed
selector has Python regression coverage. It also signs and notarizes both
candidate macOS binaries with the `public-release` environment credentials,
checks the resolved Gatekeeper verdict, attests the signed zips and checksum
manifest, and retains them as non-publishing evidence. Pull-request CI does not qualify
because GitHub tests a synthetic merge commit for that event.
The workflow uploads candidate archives and evidence but cannot create a tag or
GitHub Release.

The current Go vulnerability database also reports GO-2026-5932 at module
scope because `golang.org/x/crypto` contains the unmaintained `openpgp` package.
There is no fixed module version. Neither the desktop nor mobile package graph
imports `openpgp`, and `govulncheck` reports zero affected packages and symbols;
reviewers must recheck that reachability result on the exact candidate rather
than treating the module-only notice as either a reachable vulnerability or a
reason to suppress future findings.

GitHub artifact attestations on Free/Pro/Team require a public repository. The
candidate workflow therefore records them when the repository is public and
explicitly reports them as deferred while it is private. Publication cannot
run while the repository is private.

## Review and publishing

1. Review the candidate commit, all candidate workflow jobs, checksums, SBOMs,
   secret-scan report, native smokes, and release checklist.
2. Confirm the `public-release` environment still holds the six signing
   secrets, so only the jobs naming it can read them.
3. Create an immutable `v*` tag on the reviewed commit. Pushing it is the
   approval: everything after it runs unattended. Do not move a tag; replace a
   bad candidate with a new version.
4. Publish either way:
   - Push the `v*` tag. The push runs `.github/workflows/release.yml`
     automatically: the version and approved commit come from the tag itself,
     and the workflow discovers the successful candidate run for the tagged
     commit on its own.
   - Or run `.github/workflows/release.yml` manually with the tag, full
     reviewed commit SHA, and successful candidate workflow run ID.

Both paths execute the identical pipeline. The release workflow refuses a
private repository, a moved/mismatched tag, or a commit with no successful
candidate run. It rebuilds the final version twice from clean Go build caches
and requires byte-identical output, executes the downloaded archive on native
Linux, macOS, and Windows runners for amd64 and arm64, creates build-provenance
attestations for every published file and CycloneDX attestations for every
binary, and then creates the GitHub Release.

Verify provenance after downloading:

```sh
gh attestation verify ./queqiaod_v0.1.0_linux_amd64.tar.gz \
  --repo bojieli/queqiao
```

## macOS signing and notarization

The reproducible `tar.gz` archives are never signed. A Developer ID signature
embeds a certificate chain and an RFC 3161 timestamp, so a signed binary cannot
be rebuilt byte-for-byte by anyone who does not hold the private key, and the
repository's central claim is that anybody can rebuild a release and get the
same bytes. Signing the primary archives would trade that for Gatekeeper
convenience.

Signing is therefore additive. `scripts/sign_macos_release.sh` extracts each
`darwin` binary from its reproducible archive, signs it with hardened runtime
and a secure timestamp, notarizes it with Apple, and publishes it as a separate
`queqiaod_<version>_darwin_<arch>_signed.zip`. Verifiers keep using the
unsigned archives and their attestations; the signed zips exist for users who
download through a browser, where macOS applies quarantine and Gatekeeper
blocks an unsigned binary.

A bare Mach-O executable cannot carry a stapled notarization ticket, since
stapling is defined for `.app`, `.dmg`, `.pkg`, and `.kext` only. The ticket is
published to Apple and resolved online at first launch, which is the normal
outcome for a notarized command-line tool shipped in an archive. Verify a
signed download:

```sh
unzip queqiaod_v0.1.0_darwin_arm64_signed.zip
codesign --verify --strict --verbose=2 queqiaod
spctl --assess --type open --context context:primary-signature -vvv queqiaod
```

`codesign` must report the `Developer ID Application` authority, and `spctl`
must report `source=Notarized Developer ID`. The assessment type matters:
`--type exec` rejects every bare command-line tool, notarized or not, because
it looks for something it can launch as an application, so it cannot tell the
two apart. `--type open --context context:primary-signature` asks the question
Gatekeeper actually asks of a quarantined download. A signed but un-notarized
binary reports `source=Unnotarized Developer ID` there, which is what makes it
a real check rather than a formality.

The signed zips also carry build-provenance attestations.

Signing runs in the `public-release` environment and needs six secrets there:

| Secret | Contents |
| --- | --- |
| `APPLE_CERTIFICATE_P12` | base64 of a `.p12` holding the Developer ID Application identity and its chain |
| `APPLE_CERTIFICATE_PASSWORD` | export password for that `.p12` |
| `APPLE_SIGNING_IDENTITY` | identity common name, e.g. `Developer ID Application: NAME (TEAMID)` |
| `APPLE_API_KEY_P8` | base64 of the App Store Connect `AuthKey_<KEYID>.p8` |
| `APPLE_API_KEY_ID` | App Store Connect key ID |
| `APPLE_API_ISSUER_ID` | App Store Connect issuer UUID |

Export the certificate without exporting unrelated identities from the same
keychain:

```sh
security find-identity -v -p codesigning
# Export only the Developer ID identity, then confirm what the file contains:
openssl pkcs12 -in devid.p12 -nokeys -legacy | grep -E 'friendlyName|subject='
base64 -i devid.p12 | tr -d '\n' | gh secret set APPLE_CERTIFICATE_P12 \
  --env public-release
base64 -i AuthKey_XXXXXXXXXX.p8 | tr -d '\n' | gh secret set APPLE_API_KEY_P8 \
  --env public-release
```

The App Store Connect key must hold the Developer ID role; a key limited to
App Store distribution cannot notarize. Apple issues the `.p8` exactly once at
creation and it cannot be re-downloaded, so a lost key is replaced by revoking
it and issuing a new one.

If any of the six secrets is absent, candidate qualification and final
publication stop at the signing job. Missing credentials can never silently
produce a release with a different or unsigned macOS asset set.

The signing keys live in `public-release` and publication does too, so both
jobs name that environment. Keep them there: an environment scopes its secrets
to the jobs that name it, and moving them to repository-level secrets would
hand them to every workflow that runs.

The environment carries no deployment approval. Publication is gated, but by
the tag rather than by a prompt: only a maintainer can create a `v*` tag, the
`protect release tags` ruleset makes it immutable once created, and
`release.yml`'s authorize job refuses any tag whose commit has no successful
candidate run. A reviewer prompt on top of that asked a maintainer to approve
a pipeline they had already approved by pushing the tag.

What that trades away is worth naming: nobody now inspects the notarization
result between signing and publishing. If that inspection matters to you,
restore the approval on the `publish` job alone -- it is the one placed after
signing has reported -- rather than on both.
