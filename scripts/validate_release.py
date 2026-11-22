#!/usr/bin/env python3
"""Validate a complete queqiao release directory without extracting archives."""

from __future__ import annotations

import argparse
import datetime
import hashlib
import io
import json
import pathlib
import re
import stat
import tarfile
import zipfile


REQUIRED_ARCHIVE_FILES = {
    "BUILDINFO",
    "CHANGELOG.md",
    "CONTRIBUTING.md",
    "LICENSE",
    "PRIVACY.md",
    "README.md",
    "SBOM.cdx.json",
    "SECURITY.md",
    "THIRD_PARTY_LICENSES.txt",
    "THIRD_PARTY_NOTICES.md",
    "assets/queqiao-icon.png",
    "deploy/clash-queqiao.yaml",
    "deploy/install-client.sh",
    "deploy/install-server.sh",
    "deploy/queqiaod.service",
    "deploy/tune-server.sh",
    "docs/ARCHITECTURE.md",
    "docs/BENCHMARKING.md",
    "docs/CONTRIBUTING-NETWORK-EVIDENCE.md",
    "docs/DEPLOYING.md",
    "docs/DESIGN.md",
    "docs/FIELD-VALIDATION.md",
    "docs/KNOWN-LIMITATIONS.md",
    "docs/LOGGING.md",
    "docs/MOBILE.md",
    "docs/PATH-CHARACTER-20260813.md",
    "docs/PRODUCTION-DESIGN.md",
    "docs/PROTOCOL.md",
    "docs/README.md",
    "docs/RELEASE-CHECKLIST.md",
    "docs/RELEASING.md",
    "docs/ROADMAP.md",
    "docs/STATUS.md",
    "docs/VISION.md",
    "docs/archive/README.md",
    "docs/archive/2026-08-development/DESIGN-ERASURE.md",
    "docs/archive/2026-08-development/DESIGN-MULTIPATH.md",
    "docs/archive/2026-08-development/MEASUREMENTS-20260809.md",
    "docs/archive/2026-08-development/MEASUREMENTS-20260810.md",
    "docs/archive/2026-08-development/MEASUREMENTS-20260816.md",
    "docs/archive/2026-08-development/PERFORMANCE-20260812.md",
    "docs/archive/2026-08-development/PROFILE-20260811.md",
    "docs/archive/2026-08-development/PUBLIC-HISTORY-AUDIT-20260817.md",
    "docs/archive/2026-08-development/README.md",
    "docs/archive/2026-08-development/RELEASE-CANDIDATE-20260817.md",
    "docs/archive/2026-08-development/RELEASE-HARDENING-20260817.md",
    "docs/archive/2026-08-development/STALL-20260817.md",
    "docs/archive/2026-08-development/STATIC-SECURITY-AUDIT-20260817.md",
    "docs/archive/2026-08-development/TCP-FALLBACK-20260817.md",
    "docs/archive/2026-08-development/field-results/20260817-primary-high-port.md",
    "docs/field-results/README.md",
    "internal/congestion/NOTICE",
}

# The deployment guide in the archive tells the operator to run these, so they
# must arrive executable rather than needing a chmod the guide does not mention.
EXECUTABLE_ARCHIVE_FILES = {
    "deploy/install-client.sh",
    "deploy/install-server.sh",
    "deploy/tune-server.sh",
}
CHECKSUM = re.compile(r"[0-9a-f]{64}")
FULL_COMMIT = re.compile(r"[0-9a-f]{40}")
SAFE_METADATA = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+~-]*")
TARGET = re.compile(r"[a-z0-9]+/[a-z0-9]+")
GO_VERSION = re.compile(r"go[0-9]+\.[0-9]+\.[0-9]+")
UTC_BUILD_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z")
NOTICE_ROW = re.compile(r"^\| `([^`]+)` \| ([^ |]+) \| ([A-Za-z0-9.-]+) \|$")
EXPECTED_TARGETS = {
    "darwin/amd64",
    "darwin/arm64",
    "linux/amd64",
    "linux/arm64",
    "windows/amd64",
    "windows/arm64",
}
BUILDINFO_KEYS = {
    "version",
    "commit",
    "build_date",
    "target",
    "go",
    "wire_protocol",
    "binary_sha256",
}


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def parse_checksums(path: pathlib.Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        fields = line.split("  ", 1)
        if (
            len(fields) != 2
            or CHECKSUM.fullmatch(fields[0]) is None
            or not fields[1]
            or "/" in fields[1]
            or "\\" in fields[1]
        ):
            raise ValueError(f"invalid SHA256SUMS line {number}")
        if fields[1] in result:
            raise ValueError(f"duplicate checksum entry {fields[1]}")
        result[fields[1]] = fields[0]
    return result


def archive_relative(name: str, expected_root: str) -> str:
    if "\\" in name:
        raise ValueError(f"unsafe archive member {name!r}")
    member = pathlib.PurePosixPath(name)
    parts = member.parts
    if member.is_absolute() or len(parts) < 2 or parts[0] != expected_root:
        raise ValueError(f"unsafe archive member {name!r}")
    if any(part in {"", ".", ".."} for part in parts):
        raise ValueError(f"unsafe archive member {name!r}")
    return "/".join(parts[1:])


def archive_files(path: pathlib.Path, expected_root: str) -> dict[str, tuple[bytes, int]]:
    result: dict[str, tuple[bytes, int]] = {}
    if path.suffix == ".zip":
        with zipfile.ZipFile(path) as archive:
            for entry in archive.infolist():
                if entry.is_dir():
                    raise ValueError(f"unexpected archive directory {entry.filename!r}")
                raw_mode = entry.external_attr >> 16
                if entry.create_system != 3 or stat.S_IFMT(raw_mode) != stat.S_IFREG:
                    raise ValueError(f"unexpected non-file ZIP member {entry.filename!r}")
                relative = archive_relative(entry.filename, expected_root)
                if relative in result:
                    raise ValueError(f"duplicate archive member {relative!r}")
                mode = (entry.external_attr >> 16) & 0o777
                result[relative] = (archive.read(entry), mode)
        return result
    with tarfile.open(path, "r:gz") as archive:
        for entry in archive.getmembers():
            if not entry.isfile():
                raise ValueError(f"unexpected non-file archive member {entry.name!r}")
            relative = archive_relative(entry.name, expected_root)
            if relative in result:
                raise ValueError(f"duplicate archive member {relative!r}")
            reader = archive.extractfile(entry)
            if reader is None:
                raise ValueError(f"cannot read archive member {entry.name!r}")
            result[relative] = (reader.read(), entry.mode & 0o777)
    return result


def properties(component: dict) -> dict[str, str]:
    result: dict[str, str] = {}
    for item in component.get("properties", []):
        if (
            not isinstance(item, dict)
            or not isinstance(item.get("name"), str)
            or not isinstance(item.get("value"), str)
            or item["name"] in result
        ):
            raise ValueError("SBOM contains an invalid or duplicate property")
        result[item["name"]] = item["value"]
    return result


def parse_buildinfo(data: bytes) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in data.decode("utf-8").splitlines():
        key, separator, value = line.partition("=")
        if not separator or key in result:
            raise ValueError("invalid BUILDINFO")
        result[key] = value
    if set(result) != BUILDINFO_KEYS or any(not value for value in result.values()):
        raise ValueError("BUILDINFO keys or values are incomplete")
    if SAFE_METADATA.fullmatch(result["version"]) is None:
        raise ValueError("BUILDINFO version is not release-safe")
    if FULL_COMMIT.fullmatch(result["commit"]) is None:
        raise ValueError("BUILDINFO commit is not a full lowercase SHA")
    if UTC_BUILD_DATE.fullmatch(result["build_date"]) is None:
        raise ValueError("BUILDINFO build date is not canonical UTC RFC3339")
    try:
        build_date = datetime.datetime.strptime(
            result["build_date"], "%Y-%m-%dT%H:%M:%SZ"
        )
    except ValueError as error:
        raise ValueError("BUILDINFO build date is invalid") from error
    if build_date.year < 1980:
        raise ValueError("BUILDINFO build date predates ZIP support")
    if TARGET.fullmatch(result["target"]) is None:
        raise ValueError("BUILDINFO target is invalid")
    if GO_VERSION.fullmatch(result["go"]) is None:
        raise ValueError("BUILDINFO Go version is not a patched release toolchain")
    wire = result["wire_protocol"]
    if re.fullmatch(r"[1-9][0-9]{0,2}", wire) is None or int(wire) > 255:
        raise ValueError("BUILDINFO wire protocol is not a positive version byte")
    if CHECKSUM.fullmatch(result["binary_sha256"]) is None:
        raise ValueError("BUILDINFO binary hash is invalid")
    return result


def parse_notice_rows(data: bytes) -> set[tuple[str, str, str]]:
    rows: set[tuple[str, str, str]] = set()
    for line in data.decode("utf-8").splitlines():
        match = NOTICE_ROW.fullmatch(line)
        if match is None:
            continue
        row = match.groups()
        if row in rows:
            raise ValueError(f"duplicate third-party notice row for {row[0]}")
        rows.add(row)
    if not rows:
        raise ValueError("third-party notice contains no module rows")
    return rows


def validate_notice_summary(
    data: bytes, module_components: list[dict], archive_name: str
) -> None:
    expected = {
        (
            item["name"],
            item["version"],
            item["licenses"][0]["license"]["id"],
        )
        for item in module_components
    }
    actual = parse_notice_rows(data)
    if actual != expected:
        raise ValueError(
            f"{archive_name}: third-party notice differs from linked modules: "
            f"missing={sorted(expected - actual)} unexpected={sorted(actual - expected)}"
        )


def validate_sbom(
    data: bytes, archive_name: str, archive: dict[str, tuple[bytes, int]]
) -> list[dict]:
    bom = json.loads(data)
    if bom.get("bomFormat") != "CycloneDX" or bom.get("specVersion") != "1.5":
        raise ValueError(f"{archive_name}: unsupported SBOM identity")
    component = bom.get("metadata", {}).get("component", {})
    if component.get("type") != "application" or component.get("name") != "queqiaod":
        raise ValueError(f"{archive_name}: invalid root SBOM component")
    if component.get("licenses") != [{"license": {"id": "MIT"}}]:
        raise ValueError(f"{archive_name}: invalid root SBOM license")
    component_properties = properties(component)
    buildinfo = parse_buildinfo(archive["BUILDINFO"][0])
    spec = archive["docs/PROTOCOL.md"][0].decode("utf-8")
    declared_wire = re.search(r"\*\*Wire version byte:\*\* `([0-9]+)`", spec)
    if declared_wire is None or declared_wire.group(1) != buildinfo["wire_protocol"]:
        raise ValueError(f"{archive_name}: protocol specification disagrees with BUILDINFO")
    for sbom_key, build_key in (
        ("queqiao:commit", "commit"),
        ("queqiao:target", "target"),
        ("queqiao:wire-protocol", "wire_protocol"),
        ("queqiao:go-version", "go"),
    ):
        if component_properties.get(sbom_key) != buildinfo.get(build_key):
            raise ValueError(f"{archive_name}: SBOM {sbom_key} disagrees with BUILDINFO")
    if component.get("version") != buildinfo.get("version"):
        raise ValueError(f"{archive_name}: SBOM version disagrees with BUILDINFO")
    if bom.get("metadata", {}).get("timestamp") != buildinfo["build_date"]:
        raise ValueError(f"{archive_name}: SBOM timestamp disagrees with BUILDINFO")

    binary_name = "queqiaod.exe" if buildinfo["target"].startswith("windows/") else "queqiaod"
    binary, mode = archive[binary_name]
    if mode & 0o111 == 0:
        raise ValueError(f"{archive_name}: binary is not executable")
    if sha256(binary) != buildinfo.get("binary_sha256"):
        raise ValueError(f"{archive_name}: binary hash disagrees with BUILDINFO")
    hash_items = component.get("hashes", [])
    hashes = {
        item.get("alg"): item.get("content")
        for item in hash_items
        if isinstance(item, dict)
    }
    if (
        len(hash_items) != 1
        or len(hashes) != len(hash_items)
        or hashes.get("SHA-256") != buildinfo["binary_sha256"]
    ):
        raise ValueError(f"{archive_name}: binary hash disagrees with SBOM")

    module_components = bom.get("components", [])
    modules = {item["name"] for item in module_components}
    component_refs = {item["bom-ref"] for item in module_components}
    if not modules or len(modules) != len(module_components) or len(component_refs) != len(module_components):
        raise ValueError(f"{archive_name}: SBOM has no linked modules")
    for item in module_components:
        licenses = item.get("licenses")
        if (
            not isinstance(licenses, list)
            or len(licenses) != 1
            or not isinstance(licenses[0], dict)
            or set(licenses[0]) != {"license"}
            or not isinstance(licenses[0]["license"], dict)
            or set(licenses[0]["license"]) != {"id"}
            or not isinstance(licenses[0]["license"]["id"], str)
            or not licenses[0]["license"]["id"]
        ):
            raise ValueError(
                f"{archive_name}: an SBOM module lacks one unambiguous SPDX license"
            )
    license_bundle = archive["THIRD_PARTY_LICENSES.txt"][0].decode("utf-8")
    missing_licenses = sorted(module for module in modules if module not in license_bundle)
    if missing_licenses:
        raise ValueError(f"{archive_name}: license bundle omits {missing_licenses}")
    root_ref = component.get("bom-ref")
    dependency_items = bom.get("dependencies", [])
    dependencies = {
        item["ref"]: set(item.get("dependsOn", [])) for item in dependency_items
    }
    if len(dependencies) != len(dependency_items):
        raise ValueError(f"{archive_name}: SBOM has duplicate dependency nodes")
    if dependencies.get(root_ref) != component_refs:
        raise ValueError(f"{archive_name}: SBOM dependency graph is incomplete")
    return module_components


def validate_release(
    directory: pathlib.Path, expected_targets: set[str] = EXPECTED_TARGETS
) -> None:
    invalid_entries = sorted(
        path.name
        for path in directory.iterdir()
        if path.is_symlink() or not path.is_file()
    )
    if invalid_entries:
        raise ValueError(f"release directory contains non-regular entries: {invalid_entries}")
    checksums_path = directory / "SHA256SUMS"
    if not checksums_path.is_file():
        raise ValueError("SHA256SUMS is missing")
    expected = parse_checksums(checksums_path)
    actual_names = {path.name for path in directory.iterdir() if path.is_file() and path.name != "SHA256SUMS"}
    if actual_names != set(expected):
        raise ValueError(f"checksum coverage differs: files={sorted(actual_names)} sums={sorted(expected)}")
    for name, digest in expected.items():
        if sha256((directory / name).read_bytes()) != digest:
            raise ValueError(f"checksum mismatch for {name}")

    sboms = sorted(directory.glob("*.cdx.json"))
    if not sboms:
        raise ValueError("no CycloneDX SBOMs found")
    module_components: list[dict] = []
    notice_summaries: list[tuple[str, bytes]] = []
    buildinfos: list[dict[str, str]] = []
    for sbom_path in sboms:
        stem = sbom_path.name.removesuffix(".cdx.json")
        archive_path = directory / (stem + (".zip" if stem.endswith(("windows_amd64", "windows_arm64")) else ".tar.gz"))
        if not archive_path.is_file():
            raise ValueError(f"archive for {sbom_path.name} is missing")
        contents = archive_files(archive_path, stem)
        binary_name = "queqiaod.exe" if archive_path.suffix == ".zip" else "queqiaod"
        required = REQUIRED_ARCHIVE_FILES | {binary_name}
        if set(contents) != required:
            raise ValueError(
                f"{archive_path.name}: archive coverage differs: "
                f"missing={sorted(required - set(contents))} "
                f"unexpected={sorted(set(contents) - required)}"
            )
        for name, (_, mode) in contents.items():
            executable = name == binary_name or name in EXECUTABLE_ARCHIVE_FILES
            expected_mode = 0o755 if executable else 0o644
            if mode != expected_mode:
                raise ValueError(
                    f"{archive_path.name}: {name} mode is {mode:o}, want {expected_mode:o}"
                )
        external_sbom = sbom_path.read_bytes()
        if contents["SBOM.cdx.json"][0] != external_sbom:
            raise ValueError(f"{archive_path.name}: internal and external SBOM differ")
        module_components.extend(validate_sbom(external_sbom, archive_path.name, contents))
        notice_summaries.append(
            (archive_path.name, contents["THIRD_PARTY_NOTICES.md"][0])
        )
        buildinfo = parse_buildinfo(contents["BUILDINFO"][0])
        buildinfos.append(buildinfo)
        expected_stem = "queqiaod_{}_{}".format(
            buildinfo["version"], buildinfo["target"].replace("/", "_")
        )
        if stem != expected_stem:
            raise ValueError(f"{archive_path.name}: filename disagrees with BUILDINFO")

    archives = list(directory.glob("*.tar.gz")) + list(directory.glob("*.zip"))
    if len(archives) != len(sboms):
        raise ValueError(f"archive/SBOM count differs: {len(archives)} != {len(sboms)}")
    validate_release_cohort(buildinfos, expected_targets)
    # The public notice is shared by every target and conservatively covers the
    # union of modules linked across the release. Platform-specific dependency
    # pruning (for example, x/net on Windows) therefore cannot be checked one
    # archive at a time. Requiring exact equality with the complete release
    # union still rejects stale versions, omissions, and obsolete rows.
    for archive_name, notice in notice_summaries:
        validate_notice_summary(notice, module_components, archive_name)


def validate_release_cohort(
    buildinfos: list[dict[str, str]], expected_targets: set[str] = EXPECTED_TARGETS
) -> None:
    if not expected_targets or not expected_targets.issubset(EXPECTED_TARGETS):
        raise ValueError("expected targets must be a nonempty canonical release subset")
    targets = [item["target"] for item in buildinfos]
    if len(targets) != len(set(targets)):
        raise ValueError("release contains a duplicate target")
    if set(targets) != expected_targets:
        raise ValueError(
            "release target matrix differs: "
            f"missing={sorted(expected_targets - set(targets))} "
            f"unexpected={sorted(set(targets) - expected_targets)}"
        )
    identity_fields = ("version", "commit", "build_date", "go", "wire_protocol")
    identities = {tuple(item[field] for field in identity_fields) for item in buildinfos}
    if len(identities) != 1:
        raise ValueError("release targets do not share one provenance identity")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--expected-targets",
        default=",".join(sorted(EXPECTED_TARGETS)),
        help="comma-separated canonical target matrix (default: all six release targets)",
    )
    parser.add_argument("directory", type=pathlib.Path)
    args = parser.parse_args()
    target_values = args.expected_targets.split(",")
    expected_targets = set(target_values)
    if "" in expected_targets or len(expected_targets) != len(target_values):
        parser.error("--expected-targets must contain unique, nonempty values")
    try:
        validate_release(args.directory.resolve(), expected_targets)
    except ValueError as error:
        parser.error(str(error))
    print(f"validated release artifacts in {args.directory}")


if __name__ == "__main__":
    main()
