#!/usr/bin/env python3
"""Sign release binaries with the Ed25519 PEM in AGENT_SIGNING_PRIVATE_KEY."""
import os
from pathlib import Path
import subprocess
import tempfile


def run(*args):
    return subprocess.run(args, check=True, capture_output=True).stdout


def main():
    root = Path(__file__).resolve().parent.parent
    expected = bytes.fromhex((root / "release-signing-public-key.hex").read_text().strip())
    if len(expected) != 32:
        raise SystemExit("Expected a raw 32-byte Ed25519 public key")
    private = os.environ.get("AGENT_SIGNING_PRIVATE_KEY", "")
    if not private:
        raise SystemExit("AGENT_SIGNING_PRIVATE_KEY is required; refusing an unsigned release")
    with tempfile.TemporaryDirectory() as directory:
        key = Path(directory) / "key.pem"
        key.touch(mode=0o600)
        key.write_text(private)
        public = run("openssl", "pkey", "-in", str(key), "-pubout", "-outform", "DER")
        if public != bytes.fromhex("302a300506032b6570032100") + expected:
            raise SystemExit("Signing key does not match the Hub public key")
        pubfile = Path(directory) / "public.der"
        pubfile.write_bytes(public)
        for target in ("x86_64-unknown-linux-musl", "aarch64-unknown-linux-musl"):
            binary = root / "bin" / f"monitor-agent-{target}"
            signature = binary.with_name(binary.name + ".sig")
            run("openssl", "pkeyutl", "-sign", "-rawin", "-inkey", str(key),
                "-in", str(binary), "-out", str(signature))
            if signature.stat().st_size != 64:
                raise SystemExit("Expected a raw 64-byte Ed25519 signature")
            run("openssl", "pkeyutl", "-verify", "-rawin", "-pubin", "-keyform", "DER",
                "-inkey", str(pubfile), "-in", str(binary), "-sigfile", str(signature))
            print(f"Signed and verified {binary.name}")


if __name__ == "__main__":
    main()
