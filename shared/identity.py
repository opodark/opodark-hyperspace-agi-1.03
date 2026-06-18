# shared/identity.py
# HyperSpace AGI v1.02 — Node Identity + Request Signing
# Genera e persiste keypair ECDSA secp256k1 per ogni nodo.
# node_id = sha256(pubkey_bytes).hexdigest()[:40]
# v1.02: aggiunge make_request_headers / verify_request_headers per firma HTTP inter-nodo

import base64
import hashlib
import json
import os
import time
from datetime import datetime, timezone

from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.exceptions import InvalidSignature

DATA_DIR = os.getenv("DATA_DIR", "./data")
ID_FILE  = os.path.join(DATA_DIR, "node_identity.json")


def _pubkey_bytes(pub_key) -> bytes:
    return pub_key.public_bytes(
        serialization.Encoding.X962,
        serialization.PublicFormat.UncompressedPoint,
    )


def generate_or_load_identity() -> dict:
    """Genera un nuovo keypair al primo boot o ricarica quello esistente.
    Restituisce un dict con node_id, public_key (hex), created_at e _private_key.
    """
    os.makedirs(DATA_DIR, exist_ok=True)
    priv_path = os.path.join(DATA_DIR, "node_private.pem")

    if os.path.exists(ID_FILE) and os.path.exists(priv_path):
        with open(ID_FILE) as f:
            data = json.load(f)
        with open(priv_path, "rb") as f:
            private_key = serialization.load_pem_private_key(f.read(), password=None)
        data["_private_key"] = private_key
        print(f"[IDENTITY] Loaded existing node_id: {data['node_id']}")
        return data

    private_key = ec.generate_private_key(ec.SECP256K1())
    public_key  = private_key.public_key()
    pub_bytes   = _pubkey_bytes(public_key)
    node_id     = hashlib.sha256(pub_bytes).hexdigest()[:40]

    identity = {
        "node_id":    node_id,
        "public_key": pub_bytes.hex(),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    with open(ID_FILE, "w") as f:
        json.dump(identity, f, indent=2)
    with open(priv_path, "wb") as f:
        f.write(private_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ))

    identity["_private_key"] = private_key
    print(f"[IDENTITY] Generated new node_id: {node_id}")
    return identity


def sign_message(payload: dict, private_key) -> dict:
    """Firma il payload JSON con la chiave privata ECDSA del nodo.
    Restituisce il payload con campo 'signature' aggiunto.
    """
    msg = {k: v for k, v in payload.items() if k != "signature"}
    msg_bytes = json.dumps(msg, sort_keys=True, ensure_ascii=False).encode()
    sig = private_key.sign(msg_bytes, ec.ECDSA(hashes.SHA256()))
    return {**msg, "signature": sig.hex()}


def verify_message(message: dict) -> bool:
    """Verifica la firma ECDSA di un messaggio ricevuto.
    Il messaggio deve contenere i campi 'pubkey' e 'signature'.
    """
    try:
        sig_hex    = message.get("signature", "")
        pubkey_hex = message.get("pubkey", "")
        if not sig_hex or not pubkey_hex:
            return False
        pub_key = ec.EllipticCurvePublicKey.from_encoded_point(
            ec.SECP256K1(), bytes.fromhex(pubkey_hex)
        )
        msg = {k: v for k, v in message.items() if k != "signature"}
        msg_bytes = json.dumps(msg, sort_keys=True, ensure_ascii=False).encode()
        pub_key.verify(bytes.fromhex(sig_hex), msg_bytes, ec.ECDSA(hashes.SHA256()))
        return True
    except Exception:
        return False


# ── v1.02: HTTP REQUEST SIGNING ───────────────────────────────────────────────

def make_request_headers(node_id: str, pubkey_hex: str, private_key, body: bytes = b"") -> dict:
    """Genera gli header HTTP per firmare una richiesta inter-nodo.

    Header prodotti:
      X-Node-Id        : node_id del mittente
      X-Node-Pubkey    : pubkey hex del mittente
      X-Node-Timestamp : unix timestamp (intero)
      X-Node-Signature : firma ECDSA su sha256(timestamp + body)
    """
    ts = str(int(time.time()))
    body_hash = hashlib.sha256(body).digest()
    to_sign = (ts.encode() + b":") + body_hash
    sig = private_key.sign(to_sign, ec.ECDSA(hashes.SHA256()))
    return {
        "X-Node-Id":        node_id,
        "X-Node-Pubkey":    pubkey_hex,
        "X-Node-Timestamp": ts,
        "X-Node-Signature": base64.b64encode(sig).decode(),
    }


def verify_request_headers(headers: dict, body: bytes = b"", max_age_s: int = 30) -> bool:
    """Verifica gli header di firma di una richiesta inter-nodo in ingresso.

    Controlla:
    - presenza di tutti gli header richiesti
    - timestamp non troppo vecchio (replay protection, default 30s)
    - firma ECDSA valida su sha256(timestamp + body)
    """
    try:
        node_id   = headers.get("X-Node-Id", "")
        pubkey_hex = headers.get("X-Node-Pubkey", "")
        ts_str    = headers.get("X-Node-Timestamp", "")
        sig_b64   = headers.get("X-Node-Signature", "")

        if not all([node_id, pubkey_hex, ts_str, sig_b64]):
            return False

        # Replay protection
        ts = int(ts_str)
        if abs(time.time() - ts) > max_age_s:
            return False

        # Verifica firma
        body_hash = hashlib.sha256(body).digest()
        to_sign   = (ts_str.encode() + b":") + body_hash
        pub_key   = ec.EllipticCurvePublicKey.from_encoded_point(
            ec.SECP256K1(), bytes.fromhex(pubkey_hex)
        )
        pub_key.verify(base64.b64decode(sig_b64), to_sign, ec.ECDSA(hashes.SHA256()))
        return True
    except (InvalidSignature, Exception):
        return False
