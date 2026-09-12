import hashlib
import io
from pathlib import Path
import tarfile
import tempfile
import unittest
import urllib.error
from unittest.mock import patch

import install


class InstallerChecks(unittest.TestCase):
    def test_transient_download_error_retries_without_credentials(self):
        failure = urllib.error.HTTPError("https://example.test/archive", 500, "temporary", {}, None)
        with patch.object(install.urllib.request, "urlopen", side_effect=[failure, io.BytesIO(b"archive")]) as request, \
                patch.object(install.time, "sleep") as sleep:
            self.assertEqual(install.download("https://example.test/archive"), b"archive")
        self.assertEqual(request.call_count, 2)
        self.assertIsNone(request.call_args.args[0].get_header("Authorization"))
        sleep.assert_called_once_with(0.5)

    def test_retry_after_seconds_and_dates_are_honored(self):
        for value in ("60", "Thu, 01 Jan 1970 00:17:40 GMT"):
            failure = urllib.error.HTTPError("https://example.test/archive", 429, "wait", {"Retry-After": value}, None)
            with patch.object(install.urllib.request, "urlopen", side_effect=[failure, io.BytesIO(b"archive")]), \
                    patch.object(install.time, "sleep") as sleep, \
                    patch.object(install.time, "time", return_value=1000):
                self.assertEqual(install.download("https://example.test/archive"), b"archive")
            sleep.assert_called_once_with(60.0)

    def test_excessive_cooldown_is_not_retried_early(self):
        failure = urllib.error.HTTPError("https://example.test/archive", 429, "wait", {"Retry-After": "120"}, None)
        with patch.object(install.urllib.request, "urlopen", side_effect=failure) as request, \
                patch.object(install.time, "sleep") as sleep:
            with self.assertRaises(urllib.error.HTTPError):
                install.download("https://example.test/archive")
        self.assertEqual(request.call_count, 1)
        sleep.assert_not_called()

    def test_permanent_http_error_does_not_retry(self):
        for status in (401, 403, 404):
            failure = urllib.error.HTTPError("https://example.test/archive", status, "permanent", {}, None)
            with patch.object(install.urllib.request, "urlopen", side_effect=failure) as request, \
                    patch.object(install.time, "sleep") as sleep:
                with self.assertRaises(urllib.error.HTTPError):
                    install.download("https://example.test/archive")
            self.assertEqual(request.call_count, 1)
            sleep.assert_not_called()

    def test_download_retry_is_bounded(self):
        with patch.object(install.urllib.request, "urlopen", side_effect=TimeoutError("timeout")) as request, \
                patch.object(install.time, "sleep") as sleep:
            with self.assertRaises(TimeoutError):
                install.download("https://example.test/archive")
        self.assertEqual(request.call_count, 4)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [0.5, 1.0, 2.0])

    def test_partial_download_is_discarded_before_retry(self):
        with patch.object(install.urllib.request, "urlopen", side_effect=[
                install.http.client.IncompleteRead(b"partial", 100), io.BytesIO(b"complete")]), \
                patch.object(install.time, "sleep"):
            self.assertEqual(install.download("https://example.test/archive"), b"complete")

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
