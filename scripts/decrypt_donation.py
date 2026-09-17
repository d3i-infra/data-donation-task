#!/usr/bin/env python3
"""Decrypt an encrypted donation payload.

Usage:
    python scripts/decrypt_donation.py --key private_key.pem --input donation.json
    cat donation.json | python scripts/decrypt_donation.py --key private_key.pem

The envelope is a self-documenting JSON object:
    version, key_wrapping, content_encryption, key_id,
    encrypted_aes_key, iv, ciphertext

Output is the decrypted payload written to stdout.
"""
import argparse
import base64
import hashlib
import json
import sys

from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

SUPPORTED_VERSIONS = {1}
SUPPORTED_KEY_WRAPPING = {"RSA-OAEP-SHA256"}
SUPPORTED_CONTENT_ENCRYPTION = {"AES-256-GCM"}


def _check_envelope_metadata(envelope: dict) -> None:
    """Validate self-documenting metadata fields, if present."""
    version = envelope.get("version")
    if version is not None and version not in SUPPORTED_VERSIONS:
        raise ValueError(f"Unsupported envelope version: {version}")

    kw = envelope.get("key_wrapping")
    if kw is not None and kw not in SUPPORTED_KEY_WRAPPING:
        raise ValueError(f"Unsupported key_wrapping: {kw}")

    ce = envelope.get("content_encryption")
    if ce is not None and ce not in SUPPORTED_CONTENT_ENCRYPTION:
        raise ValueError(f"Unsupported content_encryption: {ce}")


def decrypt_envelope(envelope: dict, private_key) -> bytes:
    """Decrypt an encrypted donation envelope.

    Parameters
    ----------
    envelope:
        Self-documenting envelope dict (see module docstring for fields).
    private_key:
        RSA private key object (from cryptography library).

    Returns
    -------
    bytes
        The decrypted payload.
    """
    _check_envelope_metadata(envelope)

    aes_key = private_key.decrypt(
        base64.b64decode(envelope["encrypted_aes_key"]),
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None,
        ),
    )
    iv = base64.b64decode(envelope["iv"])
    ciphertext = base64.b64decode(envelope["ciphertext"])
    return AESGCM(aes_key).decrypt(iv, ciphertext, None)


def main():
    parser = argparse.ArgumentParser(description="Decrypt an encrypted donation payload.")
    parser.add_argument("--key", required=True, help="Path to RSA private key (PEM)")
    parser.add_argument("--input", help="Path to encrypted envelope JSON (default: stdin)")
    args = parser.parse_args()

    # Load private key
    with open(args.key, "rb") as f:
        private_key = serialization.load_pem_private_key(f.read(), password=None)

    # Load envelope
    if args.input:
        with open(args.input) as f:
            envelope = json.load(f)
    else:
        envelope = json.load(sys.stdin)

    # Decrypt and output
    plaintext = decrypt_envelope(envelope, private_key)
    sys.stdout.buffer.write(plaintext)


if __name__ == "__main__":
    main()
