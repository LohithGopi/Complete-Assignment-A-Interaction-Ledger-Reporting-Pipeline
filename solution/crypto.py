"""Stdlib PII protection: deterministic HMAC blind index + authenticated encryption."""
from __future__ import annotations

import base64
import hashlib
import hmac
import os
from pathlib import Path


def load_or_create_key(db_path: str | Path) -> bytes:
    """Return 32-byte key from AU_PII_KEY env var or a companion keyfile."""
    env_key = os.environ.get("AU_PII_KEY")
    if env_key:
        return hashlib.sha256(env_key.encode("utf-8")).digest()
    key_path = Path(f"{db_path}.key")
    if key_path.exists():
        return base64.urlsafe_b64decode(key_path.read_text(encoding="ascii").strip())
    key = os.urandom(32)
    key_path.parent.mkdir(parents=True, exist_ok=True)
    key_path.write_text(base64.urlsafe_b64encode(key).decode("ascii"), encoding="ascii")
    return key


def blind_index(key: bytes, canonical_phone: str) -> str:
    """Deterministic HMAC-SHA256 for exact phone lookups without plaintext storage."""
    return hmac.new(key, canonical_phone.encode("utf-8"), hashlib.sha256).hexdigest()


def mask_phone(canonical_phone: str) -> str:
    """Format +91XXXXXXXXXX as +91******XXXX."""
    return f"+91******{canonical_phone[-4:]}"


def _keystream(enc_key: bytes, nonce: bytes, length: int) -> bytes:
    blocks = []
    counter = 0
    while sum(len(b) for b in blocks) < length:
        blocks.append(hmac.new(enc_key, nonce + counter.to_bytes(4, "big"), hashlib.sha256).digest())
        counter += 1
    return b"".join(blocks)[:length]


def encrypt_pii(key: bytes, plaintext: str) -> str:
    """Encrypt PII with HMAC-CTR + Encrypt-then-MAC using stdlib primitives."""
    enc_key = hmac.new(key, b"enc", hashlib.sha256).digest()
    mac_key = hmac.new(key, b"mac", hashlib.sha256).digest()
    nonce = os.urandom(16)
    pt = plaintext.encode("utf-8")
    ks = _keystream(enc_key, nonce, len(pt))
    ct = (int.from_bytes(pt, "big") ^ int.from_bytes(ks, "big")).to_bytes(len(pt), "big") if pt else b""
    tag = hmac.new(mac_key, nonce + ct, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(nonce + ct + tag).decode("ascii")


def decrypt_pii(key: bytes, token: str) -> str:
    """Verify MAC and decrypt PII token."""
    raw = base64.urlsafe_b64decode(token.encode("ascii"))
    nonce, ct, tag = raw[:16], raw[16:-32], raw[-32:]
    enc_key = hmac.new(key, b"enc", hashlib.sha256).digest()
    mac_key = hmac.new(key, b"mac", hashlib.sha256).digest()
    expected = hmac.new(mac_key, nonce + ct, hashlib.sha256).digest()
    if not hmac.compare_digest(tag, expected):
        raise ValueError("PII MAC verification failed")
    ks = _keystream(enc_key, nonce, len(ct))
    pt = (int.from_bytes(ct, "big") ^ int.from_bytes(ks, "big")).to_bytes(len(ct), "big") if ct else b""
    return pt.decode("utf-8")
