"""Encrypt donation payloads with AES-256-GCM + RSA-OAEP.

Uses the ``cryptography`` library (available as a Pyodide package).
The encrypted envelope is a self-documenting JSON object: metadata fields
describe the algorithms and key used, so anyone holding the private key can
decrypt without access to this code.

Envelope fields
---------------
- ``version``          – envelope format version (currently ``1``).
- ``key_wrapping``     – algorithm used to wrap the AES key (``RSA-OAEP-SHA256``).
- ``content_encryption`` – cipher used on the payload (``AES-256-GCM``).
- ``encrypted_aes_key`` – base64-encoded RSA-OAEP wrapped AES key.
- ``iv``               – base64-encoded 12-byte nonce.
- ``ciphertext``       – base64-encoded AES-256-GCM ciphertext (includes tag).
"""
import base64
import json
import logging
import os

from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

logger = logging.getLogger(__name__)

ENVELOPE_VERSION = 1
KEY_WRAPPING = "RSA-OAEP-SHA256"
CONTENT_ENCRYPTION = "AES-256-GCM"


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

    # Per-donation AES-256 key + 12-byte IV
    aes_key = os.urandom(32)
    iv = os.urandom(12)

    # AES-GCM encrypt the payload
    ciphertext = AESGCM(aes_key).encrypt(iv, payload_bytes, None)

    # RSA-OAEP wrap the AES key
    wrapped_key = pub.encrypt(
        aes_key,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None,
        ),
    )

    return json.dumps({
        "version": ENVELOPE_VERSION,
        "key_wrapping": KEY_WRAPPING,
        "content_encryption": CONTENT_ENCRYPTION,
        "encrypted_aes_key": base64.b64encode(wrapped_key).decode(),
        "iv": base64.b64encode(iv).decode(),
        "ciphertext": base64.b64encode(ciphertext).decode(),
    })
