import hashlib
import io
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import install


class InstallerChecks(unittest.TestCase):
    def test_unsupported_os_is_named(self):
        with patch.object(install.platform, "system", return_value="FreeBSD"):
            with self.assertRaisesRegex(ValueError, "unsupported operating system: FreeBSD"):
                install.host_target()

    def test_uppercase_checksum_is_equivalent(self):
        data = b"archive"
        digest = hashlib.sha256(data).hexdigest()
        install.verify_archive(data, f"{digest.upper()} archive.tar.gz".encode(), "archive.tar.gz", digest)

    def test_install_uses_unique_staging_file(self):
        data = io.BytesIO()
        with tarfile.open(fileobj=data, mode="w:gz") as archive:
            member = tarfile.TarInfo("cqels-mcp")
            member.size = 3
            archive.addfile(member, io.BytesIO(b"exe"))
        payload = data.getvalue()
        digest = hashlib.sha256(payload).hexdigest()
        pin = {"version": "test", "public_repository": "cqels/CQELS-RS", "archives": {
            "target": {"filename": "archive.tar.gz", "sha256": digest}}}
        with tempfile.TemporaryDirectory() as directory:
            dest = Path(directory)
            sentinel = dest / "cqels-mcp.partial"
            sentinel.write_bytes(b"another install")
            with patch.object(install, "download", side_effect=[payload, f"{digest} archive.tar.gz".encode()]):
                output = install.install("target", dest, pin)
            self.assertEqual(output.read_bytes(), b"exe")
            self.assertEqual(sentinel.read_bytes(), b"another install")
            self.assertEqual(sorted(p.name for p in dest.iterdir()), ["cqels-mcp", "cqels-mcp.partial"])


if __name__ == "__main__":
    unittest.main()
