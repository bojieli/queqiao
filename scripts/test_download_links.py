"""The published download links must name the newest release, not a stale one."""

import pathlib
import re
import unittest


ROOT = pathlib.Path(__file__).resolve().parent.parent
# Scanned for pinned download links wherever they appear. docs/DEPLOYING.md
# links the releases page instead, which cannot go stale and needs no entry
# here; it is scanned anyway in case a pinned link is added to it later.
DOCUMENTS = ("README.md", "docs/DEPLOYING.md")
# The document that must actually offer per-platform downloads.
CATALOG = "README.md"
DOWNLOAD = re.compile(
    r"https://github\.com/bojieli/queqiao/releases/download/(?P<version>v\S+?)/"
    r"(?P<asset>[A-Za-z0-9._-]+)"
)
SECTION = re.compile(r"^## (?P<version>v\S+) - \d{4}-\d{2}-\d{2}\s*$", re.M)


class DownloadLinkTests(unittest.TestCase):
    def links(self):
        found = []
        for name in DOCUMENTS:
            text = (ROOT / name).read_text(encoding="utf-8")
            for match in DOWNLOAD.finditer(text):
                found.append((name, match.group("version"), match.group("asset")))
        return found

    def newest_release(self):
        text = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
        for section in SECTION.finditer(text):
            # A prepared release can be held by its candidate checks. Downloads
            # must keep pointing to the most recent published section.
            if not text[section.end():].lstrip().startswith("**Publication pending.**"):
                return section.group("version")
        self.fail("CHANGELOG.md records no published release")

    def test_the_catalog_offers_downloads(self):
        assets = {asset for name, _, asset in self.links() if name == CATALOG}
        for target in ("darwin_arm64", "darwin_amd64", "linux_amd64", "linux_arm64"):
            self.assertTrue(
                any(target in asset for asset in assets),
                f"{CATALOG} offers no {target} download",
            )

    def test_links_name_one_version(self):
        versions = {version for _, version, _ in self.links()}
        self.assertEqual(len(versions), 1, f"mixed download versions: {versions}")

    def test_links_name_the_newest_released_version(self):
        newest = self.newest_release()
        for name, version, asset in self.links():
            self.assertEqual(
                version,
                newest,
                f"{name} links {version}; CHANGELOG.md's newest release is "
                f"{newest}. Cutting a release means updating these links.",
            )

    def test_asset_names_carry_that_version(self):
        # SHA256SUMS is the one published asset with no version in its name.
        for name, version, asset in self.links():
            if asset.startswith("SHA256SUMS"):
                continue
            self.assertIn(version, asset, f"{name}: {asset} is not a {version} asset")


if __name__ == "__main__":
    unittest.main()
