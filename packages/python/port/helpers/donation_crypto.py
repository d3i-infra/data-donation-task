"""Donation payload encryption and decryption — AES-256-GCM + RSA-OAEP.

This module is the **single source of truth** for the encryption scheme used
to protect donation payloads.  Other repositories that need to encrypt or
decrypt donations should replicate the logic here.

Scheme overview
---------------
Hybrid encryption:  a fresh random AES-256 key encrypts the payload, and the
researcher's RSA public key wraps (encrypts) that AES key.  Only the holder
of the corresponding RSA private key can recover the AES key and decrypt.

* **Content encryption** — AES-256-GCM (authenticated encryption).
  A random 256-bit key and 96-bit (12-byte) IV are generated per donation.
  GCM produces ciphertext that includes a 128-bit authentication tag, so
  any tampering is detected on decryption.

* **Key wrapping** — RSA-OAEP with SHA-256 (both for the hash and MGF1).
  The per-donation AES key (32 bytes) is encrypted under the researcher's
  RSA public key.  Minimum key size: 2048-bit; 4096-bit recommended.

Envelope format (JSON)
----------------------
The encrypted output is a self-documenting JSON object.  A recipient with
just the private key and the envelope can determine the algorithms without
access to this codebase.

Metadata fields:

- ``version``            — envelope format version (currently ``1``).
- ``key_wrapping``       — algorithm identifier: ``"RSA-OAEP-SHA256"``.
- ``content_encryption`` — cipher identifier: ``"AES-256-GCM"``.

Crypto fields (all base64-encoded):

- ``encrypted_aes_key``  — RSA-OAEP wrapped AES-256 key.
- ``iv``                 — 12-byte nonce / initialization vector.
- ``ciphertext``         — AES-256-GCM ciphertext (includes the GCM auth tag).

Dependencies
------------
Uses the ``cryptography`` library (available as a Pyodide package, so it
runs identically in the browser and in the test runner).
"""
import base64
import json
import logging
import os
from typing import Union

from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

logger = logging.getLogger(__name__)

# ── Envelope constants ────────────────────────────────────────────────
ENVELOPE_VERSION = 1
KEY_WRAPPING = "RSA-OAEP-SHA256"
CONTENT_ENCRYPTION = "AES-256-GCM"

SUPPORTED_VERSIONS = {ENVELOPE_VERSION}
SUPPORTED_KEY_WRAPPING = {KEY_WRAPPING}
SUPPORTED_CONTENT_ENCRYPTION = {CONTENT_ENCRYPTION}

# The RSA-OAEP padding configuration, identical for encrypt and decrypt.
_RSA_OAEP_PADDING = padding.OAEP(
    mgf=padding.MGF1(algorithm=hashes.SHA256()),
    algorithm=hashes.SHA256(),
    label=None,
)


# ── Encryption ────────────────────────────────────────────────────────

def encrypt_payload(payload_bytes: bytes, public_key_pem: str) -> str:
    """Encrypt *payload_bytes* for the researcher's RSA public key.

    Parameters
    ----------
    payload_bytes:
        The raw donation payload (typically UTF-8 JSON).
    public_key_pem:
        RSA public key in PEM format (SPKI).  Minimum 2048-bit; 4096-bit
        recommended.

    Returns
    -------
    str
        Self-documenting JSON envelope (see module docstring for fields).
    """
    logger.info("Encrypting donation payload (%d bytes)", len(payload_bytes))

    pub = serialization.load_pem_public_key(public_key_pem.encode())
    if not isinstance(pub, rsa.RSAPublicKey):
        raise ValueError("Expected an RSA public key")

    # Per-donation AES-256 key + 12-byte IV (cryptographically random)
    aes_key = os.urandom(32)
    iv = os.urandom(12)

    # AES-256-GCM encrypt the payload (ciphertext includes auth tag)
    ciphertext = AESGCM(aes_key).encrypt(iv, payload_bytes, None)

    # RSA-OAEP wrap the AES key so only the private-key holder can recover it
    wrapped_key = pub.encrypt(aes_key, _RSA_OAEP_PADDING)

    return json.dumps({
        "version": ENVELOPE_VERSION,
        "key_wrapping": KEY_WRAPPING,
        "content_encryption": CONTENT_ENCRYPTION,
        "encrypted_aes_key": base64.b64encode(wrapped_key).decode(),
        "iv": base64.b64encode(iv).decode(),
        "ciphertext": base64.b64encode(ciphertext).decode(),
    })


# ── Decryption ────────────────────────────────────────────────────────

def validate_envelope(envelope: dict) -> None:
    """Validate envelope metadata fields.

    Raises ``ValueError`` if a metadata field is present but has an
    unsupported value.  Missing fields are tolerated for forward
    compatibility.
    """
    version = envelope.get("version")
    if version is not None and version not in SUPPORTED_VERSIONS:
        raise ValueError(f"Unsupported envelope version: {version}")

    kw = envelope.get("key_wrapping")
    if kw is not None and kw not in SUPPORTED_KEY_WRAPPING:
        raise ValueError(f"Unsupported key_wrapping: {kw}")

    ce = envelope.get("content_encryption")
    if ce is not None and ce not in SUPPORTED_CONTENT_ENCRYPTION:
        raise ValueError(f"Unsupported content_encryption: {ce}")


def decrypt_envelope(
    envelope: dict,
    private_key: Union[rsa.RSAPrivateKey, "Any"],
) -> bytes:
    """Decrypt an encrypted donation envelope.

    Parameters
    ----------
    envelope:
        Parsed JSON envelope dict (see module docstring for fields).
    private_key:
        RSA private key object (``cryptography`` library type).

    Returns
    -------
    bytes
        The decrypted payload.

    Raises
    ------
    ValueError
        If envelope metadata indicates an unsupported algorithm or version.
    cryptography.exceptions.InvalidTag
        If the ciphertext has been tampered with (GCM authentication failure).
    """
    validate_envelope(envelope)

    # RSA-OAEP unwrap the per-donation AES key
    aes_key = private_key.decrypt(
        base64.b64decode(envelope["encrypted_aes_key"]),
        _RSA_OAEP_PADDING,
    )

    iv = base64.b64decode(envelope["iv"])
    ciphertext = base64.b64decode(envelope["ciphertext"])

    # AES-256-GCM decrypt (raises InvalidTag on tampering)
    return AESGCM(aes_key).decrypt(iv, ciphertext, None)
