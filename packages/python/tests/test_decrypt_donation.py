"""Tests for scripts/decrypt_donation.py — round-trip with encrypt_payload."""
import json
import subprocess
import sys
from pathlib import Path

from port.helpers.donation_crypto import encrypt_payload

FIXTURES = Path(__file__).parent / "fixtures"
SCRIPT = Path(__file__).parents[3] / "scripts" / "decrypt_donation.py"


def _load_public_key_pem() -> str:
    return (FIXTURES / "test_pubkey.pem").read_text()


class TestDecryptDonationCLI:
    """Run the actual CLI script and verify it decrypts correctly."""

    def test_round_trip_via_stdin(self):
        pubkey_pem = _load_public_key_pem()
        payload = b'{"participant": "data", "rows": [1, 2, 3]}'

        envelope = encrypt_payload(payload, pubkey_pem)

        result = subprocess.run(
            [sys.executable, str(SCRIPT), "--key", str(FIXTURES / "test_keypair.pem")],
            input=envelope.encode(),
            capture_output=True,
        )

        assert result.returncode == 0
        assert result.stdout == payload

    def test_round_trip_via_file(self, tmp_path):
        pubkey_pem = _load_public_key_pem()
        payload = b'{"hello": "world"}'

        envelope_path = tmp_path / "envelope.json"
        envelope_path.write_text(encrypt_payload(payload, pubkey_pem))

        result = subprocess.run(
            [
                sys.executable, str(SCRIPT),
                "--key", str(FIXTURES / "test_keypair.pem"),
                "--input", str(envelope_path),
            ],
            capture_output=True,
        )

        assert result.returncode == 0
        assert result.stdout == payload
