#!/usr/bin/env python3
"""Fetch a crate's source from a GitHub tag and install it into a cargo
directory-source vendor tree (with a .cargo-checksum.json of real sha256s).

This is a workaround for sandboxes where crates.io is unreachable but GitHub
is: crates are fetched from their upstream GitHub repositories instead.

Usage:
  vendor_fetch.py VENDOR_DIR REPO TAG SUBDIR_IN_TARBALL CRATE_NAME CRATE_VERSION
"""
import hashlib
import json
import os
import shutil
import sys
import tarfile
import tempfile
import urllib.request


def sha256_file(path, chunk=1 << 20):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def main():
    vendor_dir, repo, tag, subdir, name, version = sys.argv[1:7]
    url = f"https://codeload.github.com/{repo}/tar.gz/refs/tags/{tag}"
    dest = os.path.join(vendor_dir, f"{name}-{version}")
    if os.path.isdir(dest):
        print(f"skip existing {dest}")
        return
    os.makedirs(vendor_dir, exist_ok=True)
    with tempfile.TemporaryDirectory() as td:
        tarball = os.path.join(td, "src.tar.gz")
        print(f"fetch {repo}@{tag} -> {name}-{version}")
        req = urllib.request.Request(url, headers={"User-Agent": "mycelium-vendor"})
        with urllib.request.urlopen(req, timeout=600) as r, open(tarball, "wb") as f:
            while True:
                b = r.read(1 << 20)
                if not b:
                    break
                f.write(b)
        with tarfile.open(tarball, "r:gz") as tf:
            root = tf.getnames()[0].split("/", 1)[0]
            src = os.path.join(td, "extract")
            os.makedirs(src, exist_ok=True)
            tf.extractall(src)
            src = os.path.join(src, root, subdir)
            if not os.path.isdir(src):
                raise SystemExit(f"subdir {subdir!r} not found in {repo}@{tag}")
            shutil.copytree(src, dest)
    files = {}
    for dirpath, _, filenames in os.walk(dest):
        for fn in filenames:
            p = os.path.join(dirpath, fn)
            rel = os.path.relpath(p, dest)
            if rel == ".cargo-checksum.json":
                continue
            files[rel] = sha256_file(p)
    checksum = {"files": files, "package": None}
    with open(os.path.join(dest, ".cargo-checksum.json"), "w") as f:
        json.dump(checksum, f, sort_keys=True)
    print(f"vendored {name}-{version}: {len(files)} files")


if __name__ == "__main__":
    main()
