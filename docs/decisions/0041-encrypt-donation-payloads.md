---
status: proposed
date: "2026-09-11"
category: Architecture
applies_to:
    - packages/python/port/helpers/flow_builder.py
    - packages/python/port/helpers/donation_crypto.py
    - packages/python/port/configs/*_config.json
priority: invariant
---

# Encrypt donation payloads

## Decision

When a RSA public key is present in the platform config, `FlowBuilder` encrypts the donation payload before yielding the donate command — a per-donation AES-256-GCM key encrypts the data, RSA-OAEP (SHA-256) wraps that key — so the server stores only ciphertext the researcher's private key can unlock.

## Guidance

- All encryption and decryption logic lives in `packages/python/port/helpers/donation_crypto.py` — the single source of truth. It exports `encrypt_payload()`, `decrypt_envelope()`, `validate_envelope()`, and the envelope constants. The `cryptography` library (available as a Pyodide package) provides the primitives. `FlowBuilder.start_flow()` calls `encrypt_payload` when a public key is configured and yields the resulting envelope instead of plaintext.
- The encrypted envelope is a self-documenting JSON object. Metadata fields (`version`, `key_wrapping`, `content_encryption`, `key_id`) describe how to decrypt; crypto fields (`encrypted_aes_key`, `iv`, `ciphertext`) carry the base64-encoded material. A recipient with just the private key and the envelope can determine the algorithms without access to this codebase.
- Activation is conditional on the public key's presence in the platform config — no feature flags, no environment switches. No key = plaintext donation, unchanged from today. The field is `platform_info.public_key_pem` in `configs/<platform>_config.json`: set it to a PEM string to enable encryption, or `null` / omit it entirely to donate plaintext. The config validator checks format when the field is present.
- RSA key minimum is 2048-bit; 4096-bit is recommended. The public key must be PEM-encoded (SPKI format, `-----BEGIN PUBLIC KEY-----`). The config generator does not produce this field — it is added manually per deployment after the researcher generates a keypair.
- CommandRouter, Bridge, and host are unchanged — they carry the payload opaquely.
- A standalone Python decryption CLI (`scripts/decrypt_donation.py`) using the `cryptography` library serves as the reference decryption implementation; the script selector is the intended integration point for user-friendly key generation.
- Unit tests exercise the real implementation with no mocks — encrypt with `encrypt_payload`, decrypt with the `cryptography` library, assert round-trip correctness. Commit a test-only RSA keypair (not for production).

## Why

Donated data can contain sensitive personal information. Encrypting before donation ensures that neither the hosting platform nor anyone with server access can read the data — only the researcher holding the private key can decrypt it. This is a privacy boundary that must not silently degrade: if a public key is configured and encryption fails or is bypassed, the participant's data is exposed. The `cryptography` library is used because it is available as a Pyodide package, runs identically in the browser and in the test runner, and avoids the async Promise-resolution complexities of the Web Crypto JS bridge.
