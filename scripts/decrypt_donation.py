#!/usr/bin/env python3
"""Decrypt an encrypted donation payload.

Usage:
    python scripts/decrypt_donation.py --key private_key.pem --input donation.json
    cat donation.json | python scripts/decrypt_donation.py --key private_key.pem

The envelope is a self-documenting JSON object — see
``port.helpers.donation_crypto`` for the full format specification.

Output is the decrypted payload written to stdout.
"""
import argparse
import importlib.util
import json
import sys
from pathlib import Path

from cryptography.hazmat.primitives import serialization

# Import donation_crypto directly (bypassing port/__init__.py which
# depends on Pyodide's js module, unavailable outside the browser).
_CRYPTO_PATH = Path(__file__).resolve().parents[1] / "packages" / "python" / "port" / "helpers" / "donation_crypto.py"
_spec = importlib.util.spec_from_file_location("donation_crypto", _CRYPTO_PATH)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
decrypt_envelope = _mod.decrypt_envelope


def main():
    parser = argparse.ArgumentParser(description="Decrypt an encrypted donation payload.")
    parser.add_argument("--key", required=True, help="Path to RSA private key (PEM)")
    parser.add_argument("--input", help="Path to encrypted envelope JSON (default: stdin)")
    args = parser.parse_args()

    with open(args.key, "rb") as f:
        private_key = serialization.load_pem_private_key(f.read(), password=None)

    if args.input:
        with open(args.input) as f:
            envelope = json.load(f)
    else:
        envelope = json.load(sys.stdin)

    plaintext = decrypt_envelope(envelope, private_key)
    sys.stdout.buffer.write(plaintext)


if __name__ == "__main__":
    main()
