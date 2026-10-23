#!/usr/bin/env python3
"""Exercise the release signer without production credentials."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


def main():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        (root / "scripts").mkdir()
        shutil.copy(Path(__file__).with_name("sign-release.py"), root / "scripts")
        key = root / "test.pem"
        subprocess.run(["openssl", "genpkey", "-algorithm", "ED25519", "-out", str(key)], check=True)
        public = subprocess.check_output(["openssl", "pkey", "-in", str(key), "-pubout", "-outform", "DER"])
        pin = root / "release-signing-public-key.hex"
        pin.write_text(public[-32:].hex())
        (root / "bin").mkdir()
        binaries = [root / "bin" / f"monitor-agent-{arch}-unknown-linux-musl"
                    for arch in ("x86_64", "aarch64")]
        for binary in binaries:
            binary.write_bytes(b"test release payload")
        env = dict(os.environ, RELEASE_SIGNING_PRIVATE_KEY=key.read_text())
        command = ["python3", str(root / "scripts/sign-release.py")]
        subprocess.run(command, env=env, check=True)
        pubfile = root / "public.der"
        pubfile.write_bytes(public)
        for binary in binaries:
            binary.write_bytes(b"tampered payload")
            result = subprocess.run(["openssl", "pkeyutl", "-verify", "-rawin", "-pubin",
                                     "-keyform", "DER", "-inkey", str(pubfile), "-in", str(binary),
                                     "-sigfile", str(binary) + ".sig"], capture_output=True)
            assert result.returncode != 0, "tampered binary accepted"
        pin.write_text("00" * 32)
        assert subprocess.run(command, env=env, capture_output=True).returncode != 0
        env.pop("RELEASE_SIGNING_PRIVATE_KEY")
        assert subprocess.run(command, env=env, capture_output=True).returncode != 0
        print("PASS: signatures, tampering, wrong key, missing secret")


if __name__ == "__main__":
    main()
