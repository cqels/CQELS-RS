#!/usr/bin/env python3
"""Download a pinned CQELS-RS archive, verify it, and extract only the executable."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import tarfile
import tempfile
import urllib.error
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[1]


def download(url):
    request = urllib.request.Request(url, headers={"User-Agent": "cqels-public-distribution/1"})
    with urllib.request.urlopen(request, timeout=60) as response:
        return response.read()


def host_target():
    machine = platform.machine().lower()
    architecture = {"arm64": "aarch64", "amd64": "x86_64"}.get(machine, machine)
    suffix = {"Darwin": "apple-darwin", "Linux": "unknown-linux-gnu",
              "Windows": "pc-windows-msvc"}.get(platform.system())
    if suffix is None:
        raise ValueError(f"unsupported operating system: {platform.system()}")
    return f"{architecture}-{suffix}"


def verify_archive(data, checksum, filename, expected):
    actual = hashlib.sha256(data).hexdigest()
    fields = checksum.decode("utf-8-sig").strip().split()
    if len(fields) != 2 or fields[1].lstrip("*") != filename:
        raise ValueError("checksum must identify exactly the downloaded archive")
    if fields[0].lower() != expected.lower() or actual != expected.lower():
        raise ValueError(f"SHA-256 mismatch for {filename}: expected {expected}, received {actual}")


def executable_bytes(data, filename):
    executable = "cqels-mcp.exe" if filename.endswith(".zip") else "cqels-mcp"
    if filename.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            matches = [i for i in archive.infolist() if i.filename in (executable, "./" + executable)]
            if len(matches) != 1:
                raise ValueError("archive must contain exactly one root executable")
            payload = archive.read(matches[0])
    else:
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
            matches = [i for i in archive if i.name in (executable, "./" + executable)]
            if len(matches) != 1 or not matches[0].isfile():
                raise ValueError("archive must contain exactly one regular root executable")
            payload = archive.extractfile(matches[0]).read()
    if not payload:
        raise ValueError("empty executable")
    return executable, payload


def install(target, destination, pin=None):
    pin = pin or json.loads((ROOT / "RELEASE.json").read_text(encoding="utf-8"))
    if target not in pin["archives"]:
        raise ValueError(f"unsupported target {target}; available: {', '.join(pin['archives'])}")
    artifact = pin["archives"][target]
    filename = artifact["filename"]
    base = f"https://github.com/{pin['public_repository']}/releases/download/v{pin['version']}/"
    archive = download(base + filename)
    verify_archive(archive, download(base + filename + ".sha256"), filename, artifact["sha256"])
    name, payload = executable_bytes(archive, filename)
    destination.mkdir(parents=True, exist_ok=True)
    output = destination / name
    fd, temporary_name = tempfile.mkstemp(prefix=name + ".", suffix=".partial", dir=destination)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(payload)
        temporary.chmod(0o755)
        os.replace(temporary, output)
    finally:
        if temporary.exists():
            temporary.unlink()
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", help="override the detected platform target")
    parser.add_argument("--output", type=Path, default=Path(".cqels/bin"))
    args = parser.parse_args()
    try:
        print(f"Verified and installed: {install(args.target or host_target(), args.output)}")
    except (OSError, ValueError, urllib.error.URLError) as exc:
        parser.exit(1, f"Installation failed: {exc}\n")
