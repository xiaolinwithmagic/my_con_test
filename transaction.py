# transaction.py
import time
import json
import hashlib
from typing import Dict
from did_sim import sign_message, verify_signature  # 你前面已有 did_sim，sign/verify 返回 bytes

# ---------- helper ----------
def tx_digest_bytes(tx: Dict) -> bytes:
    """计算交易摘要（签名前/验签时使用），保证 signature 字段不参与摘要"""
    t = dict(tx)
    t.pop("signature", None)
    # 一致化序列化（字段顺序、紧凑）
    s = json.dumps(t, sort_keys=True, separators=(",", ":"))
    return s.encode()

def tx_id(tx: Dict) -> str:
    """返回交易唯一 id（hex sha256）——用于 metrics 索引"""
    return hashlib.sha256(tx_digest_bytes(tx)).hexdigest()

# ---------- creators ----------
def create_register_tx(creator: Dict, image_hash: str) -> Dict:
    tx = {
        "type": "REGISTER",
        "creator_did": creator["did"],
        "image_hash": image_hash,
        "timestamp": int(time.time() * 1000)
    }
    sig = sign_message(tx_digest_bytes(tx), creator["priv"])  # bytes
    tx["signature"] = sig.hex()
    return tx

def create_transfer_tx(from_actor: Dict, to_did: str, image_hash: str) -> Dict:
    tx = {
        "type": "TRANSFER",
        "image_hash": image_hash,
        "from_did": from_actor["did"],
        "to_did": to_did,
        "timestamp": int(time.time() * 1000)
    }
    sig = sign_message(tx_digest_bytes(tx), from_actor["priv"])
    tx["signature"] = sig.hex()
    return tx

def create_license_tx(owner: Dict, licensee_did: str, image_hash: str, terms: str = "non-commercial-use") -> Dict:
    tx = {
        "type": "LICENSE",
        "image_hash": image_hash,
        "owner_did": owner["did"],
        "licensee_did": licensee_did,
        "terms": terms,
        "timestamp": int(time.time() * 1000)
    }
    sig = sign_message(tx_digest_bytes(tx), owner["priv"])
    tx["signature"] = sig.hex()
    return tx

# ---------- verification ----------
def verify_tx_signature(tx: Dict, did_pub_lookup: Dict) -> bool:
    """
    did_pub_lookup: dict did -> public_key object (cryptography pubkey)
    """
    sig_hex = tx.get("signature")
    if not sig_hex:
        return False
    sig = bytes.fromhex(sig_hex)

    # determine signer DID by tx type
    if tx["type"] == "REGISTER":
        signer = tx.get("creator_did")
    elif tx["type"] == "TRANSFER":
        signer = tx.get("from_did")
    elif tx["type"] == "LICENSE":
        signer = tx.get("owner_did")
    else:
        return False

    pub = did_pub_lookup.get(signer)
    if not pub:
        return False
    return verify_signature(tx_digest_bytes(tx), sig, pub)
