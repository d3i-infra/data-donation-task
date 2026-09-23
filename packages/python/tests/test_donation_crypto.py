"""Tests for donation_crypto — real encryption, real decryption, no mocks.

Exercises the canonical encrypt/decrypt implementation in
``port.helpers.donation_crypto`` and verifies round-trip correctness.
"""
import base64
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from cryptography.hazmat.primitives import serialization

from port.helpers.donation_crypto import (
    CONTENT_ENCRYPTION,
    ENVELOPE_VERSION,
    KEY_WRAPPING,
    decrypt_envelope,
    encrypt_payload,
    validate_envelope,
)

FIXTURES = Path(__file__).parent / "fixtures"

METADATA_FIELDS = {"version", "key_wrapping", "content_encryption"}
CRYPTO_FIELDS = {"encrypted_aes_key", "iv", "ciphertext"}
ALL_ENVELOPE_FIELDS = METADATA_FIELDS | CRYPTO_FIELDS


def _load_private_key():
    pem = (FIXTURES / "test_keypair.pem").read_bytes()
    return serialization.load_pem_private_key(pem, password=None)


def _load_public_key_pem() -> str:
    return (FIXTURES / "test_pubkey.pem").read_text()


class TestEncryptPayloadRoundTrip:
    """Real encrypt -> real decrypt using the shared decrypt_envelope."""

    def test_round_trip_json_payload(self):
        pubkey_pem = _load_public_key_pem()
        private_key = _load_private_key()
        payload = b'{"hello": "world", "numbers": [1, 2, 3]}'

        envelope = json.loads(encrypt_payload(payload, pubkey_pem))
        decrypted = decrypt_envelope(envelope, private_key)

        assert decrypted == payload

    def test_envelope_has_expected_fields(self):
        pubkey_pem = _load_public_key_pem()
        envelope = json.loads(encrypt_payload(b'{"col": [1, 2]}', pubkey_pem))

        assert set(envelope.keys()) == ALL_ENVELOPE_FIELDS

    def test_envelope_metadata_values(self):
        pubkey_pem = _load_public_key_pem()
        envelope = json.loads(encrypt_payload(b"test", pubkey_pem))

        assert envelope["version"] == ENVELOPE_VERSION
        assert envelope["key_wrapping"] == KEY_WRAPPING
        assert envelope["content_encryption"] == CONTENT_ENCRYPTION

    def test_envelope_crypto_fields_are_valid_base64(self):
        pubkey_pem = _load_public_key_pem()
        envelope = json.loads(encrypt_payload(b"test", pubkey_pem))

        for field in ("encrypted_aes_key", "iv", "ciphertext"):
            raw = base64.b64decode(envelope[field])
            assert len(raw) > 0

    def test_each_call_produces_different_ciphertext(self):
        """Fresh AES key + IV per call -> different envelope every time."""
        pubkey_pem = _load_public_key_pem()
        payload = b"same payload"

        result1 = encrypt_payload(payload, pubkey_pem)
        result2 = encrypt_payload(payload, pubkey_pem)

        assert result1 != result2

    def test_empty_payload(self):
        pubkey_pem = _load_public_key_pem()
        private_key = _load_private_key()

        envelope = json.loads(encrypt_payload(b"", pubkey_pem))
        decrypted = decrypt_envelope(envelope, private_key)

        assert decrypted == b""

    def test_large_payload(self):
        """Verify encryption works for a realistic large donation payload (50 MB)."""
        pubkey_pem = _load_public_key_pem()
        private_key = _load_private_key()
        payload = b"x" * (50 * 1024 * 1024)

        envelope = json.loads(encrypt_payload(payload, pubkey_pem))
        decrypted = decrypt_envelope(envelope, private_key)

        assert decrypted == payload


class TestDecryptEnvelope:
    """Tests specific to the decrypt path."""

    def test_tampered_ciphertext_raises(self):
        """GCM authentication must detect tampering."""
        pubkey_pem = _load_public_key_pem()
        private_key = _load_private_key()

        envelope = json.loads(encrypt_payload(b"secret", pubkey_pem))

        # Flip a byte in the ciphertext
        raw = bytearray(base64.b64decode(envelope["ciphertext"]))
        raw[0] ^= 0xFF
        envelope["ciphertext"] = base64.b64encode(bytes(raw)).decode()

        with pytest.raises(Exception):  # InvalidTag from cryptography
            decrypt_envelope(envelope, private_key)

    def test_tampered_iv_raises(self):
        """Wrong IV must cause GCM authentication failure."""
        pubkey_pem = _load_public_key_pem()
        private_key = _load_private_key()

        envelope = json.loads(encrypt_payload(b"secret", pubkey_pem))

        raw_iv = bytearray(base64.b64decode(envelope["iv"]))
        raw_iv[0] ^= 0xFF
        envelope["iv"] = base64.b64encode(bytes(raw_iv)).decode()

        with pytest.raises(Exception):
            decrypt_envelope(envelope, private_key)


class TestValidateEnvelope:
    """Tests for envelope metadata validation."""

    def test_valid_envelope_passes(self):
        envelope = {
            "version": 1,
            "key_wrapping": "RSA-OAEP-SHA256",
            "content_encryption": "AES-256-GCM",
        }
        validate_envelope(envelope)  # should not raise

    def test_missing_metadata_tolerated(self):
        """Forward compat: missing fields are OK."""
        validate_envelope({})  # should not raise

    def test_unsupported_version_raises(self):
        with pytest.raises(ValueError, match="Unsupported envelope version"):
            validate_envelope({"version": 99})

    def test_unsupported_key_wrapping_raises(self):
        with pytest.raises(ValueError, match="Unsupported key_wrapping"):
            validate_envelope({"key_wrapping": "NONE"})

    def test_unsupported_content_encryption_raises(self):
        with pytest.raises(ValueError, match="Unsupported content_encryption"):
            validate_envelope({"content_encryption": "ROT13"})


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
