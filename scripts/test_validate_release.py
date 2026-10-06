import importlib.util
import pathlib
import stat
import tempfile
import unittest
import warnings
import zipfile


SCRIPT = pathlib.Path(__file__).with_name("validate_release.py")
SPEC = importlib.util.spec_from_file_location("validate_release", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class ValidateReleaseTests(unittest.TestCase):
    def test_readme_icon_is_required_in_every_archive(self):
        self.assertIn("README.md", MODULE.REQUIRED_ARCHIVE_FILES)
        self.assertIn("assets/queqiao-icon.png", MODULE.REQUIRED_ARCHIVE_FILES)

    def test_checksums_reject_paths_and_duplicates(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory, "SHA256SUMS")
            path.write_text("0" * 64 + "  ../artifact\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                MODULE.parse_checksums(path)
            path.write_text(("0" * 64 + "  artifact\n") * 2, encoding="utf-8")
            with self.assertRaises(ValueError):
                MODULE.parse_checksums(path)
            path.write_text("z" * 64 + "  artifact\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                MODULE.parse_checksums(path)

    def test_buildinfo_rejects_duplicate_or_incomplete_keys(self):
        with self.assertRaises(ValueError):
            MODULE.parse_buildinfo(b"version=one\nversion=two\n")
        with self.assertRaises(ValueError):
            MODULE.parse_buildinfo(b"version=one\n")

    def test_buildinfo_requires_canonical_provenance(self):
        fields = {
            "version": "v0.1.0-rc.1",
            "commit": "a" * 40,
            "build_date": "2026-08-19T04:00:00Z",
            "target": "linux/amd64",
            "go": "go1.25.13",
            "wire_protocol": "1",
            "binary_sha256": "b" * 64,
        }

        def encoded(values):
            return "".join(f"{key}={value}\n" for key, value in values.items()).encode()

        self.assertEqual(MODULE.parse_buildinfo(encoded(fields)), fields)
        upgraded = dict(fields, wire_protocol="2")
        self.assertEqual(MODULE.parse_buildinfo(encoded(upgraded)), upgraded)
        for key, invalid in (
            ("version", "../v0.1.0"),
            ("commit", "a" * 39),
            ("build_date", "2026-08-19T04:00:00+00:00"),
            ("build_date", "2026-02-30T04:00:00Z"),
            ("target", "linux/../amd64"),
            ("go", "go1.25"),
            ("go", "devel go1.25.13"),
            ("wire_protocol", "0"),
            ("wire_protocol", "256"),
            ("wire_protocol", "01"),
            ("binary_sha256", "b" * 63),
        ):
            changed = dict(fields)
            changed[key] = invalid
            with self.subTest(key=key, invalid=invalid), self.assertRaises(ValueError):
                MODULE.parse_buildinfo(encoded(changed))

    def test_release_cohort_requires_six_targets_and_one_identity(self):
        buildinfos = [
            {
                "version": "v0.1.0-rc.1",
                "commit": "a" * 40,
                "build_date": "2026-08-19T04:00:00Z",
                "target": target,
                "go": "go1.25.13",
                "wire_protocol": "1",
            }
            for target in sorted(MODULE.EXPECTED_TARGETS)
        ]
        MODULE.validate_release_cohort(buildinfos)

        linux = [item for item in buildinfos if item["target"] == "linux/amd64"]
        MODULE.validate_release_cohort(linux, {"linux/amd64"})

        with self.assertRaisesRegex(ValueError, "target matrix"):
            MODULE.validate_release_cohort(buildinfos[:-1])

        duplicate = [dict(item) for item in buildinfos]
        duplicate[-1]["target"] = duplicate[0]["target"]
        with self.assertRaisesRegex(ValueError, "duplicate target"):
            MODULE.validate_release_cohort(duplicate)

        mixed = [dict(item) for item in buildinfos]
        mixed[-1]["commit"] = "b" * 40
        with self.assertRaisesRegex(ValueError, "provenance identity"):
            MODULE.validate_release_cohort(mixed)

        with self.assertRaisesRegex(ValueError, "canonical release subset"):
            MODULE.validate_release_cohort(linux, {"plan9/amd64"})

    def test_sbom_properties_reject_duplicates(self):
        component = {
            "properties": [
                {"name": "queqiao:commit", "value": "one"},
                {"name": "queqiao:commit", "value": "two"},
            ]
        }
        with self.assertRaises(ValueError):
            MODULE.properties(component)

    def test_notice_rows_must_be_unique_and_match_the_sbom(self):
        notice = b"| `example.com/module` | v1.2.3 | MIT |\n"
        component = {
            "name": "example.com/module",
            "version": "v1.2.3",
            "licenses": [{"license": {"id": "MIT"}}],
        }
        MODULE.validate_notice_summary(notice, [component], "archive.tar.gz")
        with self.assertRaises(ValueError):
            MODULE.parse_notice_rows(notice + notice)
        stale = b"| `example.com/module` | v1.2.2 | MIT |\n"
        with self.assertRaises(ValueError):
            MODULE.validate_notice_summary(stale, [component], "archive.tar.gz")

    def test_notice_summary_matches_the_union_of_target_sboms(self):
        notice = (
            b"| `example.com/common` | v1.2.3 | MIT |\n"
            b"| `example.com/unix` | v4.5.6 | BSD-3-Clause |\n"
        )
        common = {
            "name": "example.com/common",
            "version": "v1.2.3",
            "licenses": [{"license": {"id": "MIT"}}],
        }
        unix = {
            "name": "example.com/unix",
            "version": "v4.5.6",
            "licenses": [{"license": {"id": "BSD-3-Clause"}}],
        }
        # Repeated common components from multiple targets collapse in the
        # release-wide set while the Unix-only component remains required.
        MODULE.validate_notice_summary(
            notice, [common, unix, common], "release.tar.gz"
        )

    def test_archive_rejects_absolute_and_duplicate_members(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory, "bad.zip")
            with zipfile.ZipFile(path, "w") as archive:
                archive.writestr("/root/file", b"bad")
            with self.assertRaises(ValueError):
                MODULE.archive_files(path, "root")

            link = zipfile.ZipInfo("root/link")
            link.create_system = 3
            link.external_attr = (stat.S_IFLNK | 0o777) << 16
            with zipfile.ZipFile(path, "w") as archive:
                archive.writestr(link, b"target")
            with self.assertRaisesRegex(ValueError, "non-file ZIP member"):
                MODULE.archive_files(path, "root")

            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                with zipfile.ZipFile(path, "w") as archive:
                    archive.writestr("root/file", b"one")
                    archive.writestr("root/file", b"two")
            with self.assertRaises(ValueError):
                MODULE.archive_files(path, "root")

    def test_release_directory_rejects_non_regular_entries(self):
        with tempfile.TemporaryDirectory() as directory:
            pathlib.Path(directory, "unexpected").mkdir()
            with self.assertRaisesRegex(ValueError, "non-regular entries"):
                MODULE.validate_release(pathlib.Path(directory))


if __name__ == "__main__":
    unittest.main()
