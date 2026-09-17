"""Tests for encrypt_payload — real encryption, real decryption, no mocks.

The test runner has no JS bridge, so ``encrypt_payload`` falls back to
``_py_encrypt`` (the ``cryptography``-based back-end).  Tests exercise the
actual implementation and verify round-trip correctness against the
reference decryption (same logic as ``scripts/decrypt_donation.py``).
"""
import base64
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from port.helpers.encrypt_payload import encrypt_payload

FIXTURES = Path(__file__).parent / "fixtures"

METADATA_FIELDS = {"version", "key_wrapping", "content_encryption"}
CRYPTO_FIELDS = {"encrypted_aes_key", "iv", "ciphertext"}
ALL_ENVELOPE_FIELDS = METADATA_FIELDS | CRYPTO_FIELDS


def _load_private_key():
    pem = (FIXTURES / "test_keypair.pem").read_bytes()
    return serialization.load_pem_private_key(pem, password=None)


def _load_public_key_pem() -> str:
    return (FIXTURES / "test_pubkey.pem").read_text()


def _decrypt_envelope(envelope: dict, private_key) -> bytes:
    """Reference decryption: RSA-OAEP unwrap AES key, AES-256-GCM decrypt."""
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


class TestEncryptPayloadRoundTrip:
    """Real encrypt → real decrypt, no mocks."""

    def test_round_trip_json_payload(self):
        pubkey_pem = _load_public_key_pem()
        private_key = _load_private_key()
        payload = b'{"hello": "world", "numbers": [1, 2, 3]}'

        result = encrypt_payload(payload, pubkey_pem)

        envelope = json.loads(result)
        decrypted = _decrypt_envelope(envelope, private_key)
        assert decrypted == payload

    def test_envelope_has_expected_fields(self):
        pubkey_pem = _load_public_key_pem()
        payload = b'{"col": [1, 2]}'

        result = encrypt_payload(payload, pubkey_pem)

        envelope = json.loads(result)
        assert set(envelope.keys()) == ALL_ENVELOPE_FIELDS

    def test_envelope_metadata_values(self):
        pubkey_pem = _load_public_key_pem()
        payload = b"test"

        envelope = json.loads(encrypt_payload(payload, pubkey_pem))

        assert envelope["version"] == 1
        assert envelope["key_wrapping"] == "RSA-OAEP-SHA256"
        assert envelope["content_encryption"] == "AES-256-GCM"

    def test_envelope_crypto_fields_are_valid_base64(self):
        pubkey_pem = _load_public_key_pem()
        payload = b"test"

        envelope = json.loads(encrypt_payload(payload, pubkey_pem))

        for field in ("encrypted_aes_key", "iv", "ciphertext"):
            raw = base64.b64decode(envelope[field])
            assert len(raw) > 0

    def test_each_call_produces_different_ciphertext(self):
        """Fresh AES key + IV per call → different envelope every time."""
        pubkey_pem = _load_public_key_pem()
        payload = b"same payload"

        result1 = encrypt_payload(payload, pubkey_pem)
        result2 = encrypt_payload(payload, pubkey_pem)

        assert result1 != result2

    def test_empty_payload(self):
        pubkey_pem = _load_public_key_pem()
        private_key = _load_private_key()
        payload = b""

        result = encrypt_payload(payload, pubkey_pem)

        decrypted = _decrypt_envelope(json.loads(result), private_key)
        assert decrypted == payload

    def test_large_payload(self):
        """Verify encryption works for a realistic large donation payload (50 MB)."""
        pubkey_pem = _load_public_key_pem()
        private_key = _load_private_key()
        payload = b"x" * (50 * 1024 * 1024)  # 50 MB

        result = encrypt_payload(payload, pubkey_pem)

        decrypted = _decrypt_envelope(json.loads(result), private_key)
        assert decrypted == payload


class TestLoadPublicKey:
    """Test loading public_key_pem from platform config."""

    def test_returns_none_when_no_key(self):
        from port.helpers.table_extractor import load_public_key_pem

        config = {"platform_info": {"name": "test"}, "tables": []}

        with patch("port.helpers.table_extractor.importlib.resources.files") as mock_files:
            mock_configs = MagicMock()
            mock_ref = MagicMock()
            mock_ref.read_text.return_value = json.dumps(config)
            mock_configs.__truediv__ = MagicMock(return_value=mock_ref)
            mock_files.return_value.__truediv__ = MagicMock(return_value=mock_configs)

            result = load_public_key_pem("test")

        assert result is None

    def test_returns_key_when_present(self):
        from port.helpers.table_extractor import load_public_key_pem

        fake_pem = "-----BEGIN PUBLIC KEY-----\nMIIBIjAN...\n-----END PUBLIC KEY-----"
        config = {"platform_info": {"name": "test", "public_key_pem": fake_pem}, "tables": []}

        with patch("port.helpers.table_extractor.importlib.resources.files") as mock_files:
            mock_configs = MagicMock()
            mock_ref = MagicMock()
            mock_ref.read_text.return_value = json.dumps(config)
            mock_configs.__truediv__ = MagicMock(return_value=mock_ref)
            mock_files.return_value.__truediv__ = MagicMock(return_value=mock_configs)

            result = load_public_key_pem("test")

        assert result == fake_pem
