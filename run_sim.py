# did_sim.py
import hashlib
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric.utils import (
    encode_dss_signature, decode_dss_signature
)
from cryptography.hazmat.backends import default_backend


# ---------- 工具函数 ----------
def _backend():
    """兼容不同 cryptography 版本"""
    try:
        # 新版不需要 backend
        return None
    except:
        return default_backend()


# ---------- 生成 DID 身份 ----------
def generate_did_keypair():
    """
    生成 (private_key, public_key, did)
    DID = did:sim:sha256(pubkey_bytes)
    """
    # 兼容写法
    try:
        priv = ec.generate_private_key(ec.SECP256R1())
    except TypeError:
        priv = ec.generate_private_key(ec.SECP256R1(), default_backend())

    pub = priv.public_key()

    pub_bytes = pub.public_bytes(
        encoding=serialization.Encoding.X962,
        format=serialization.PublicFormat.UncompressedPoint
    )

    did = "did:sim:" + hashlib.sha256(pub_bytes).hexdigest()
    return priv, pub, did


# ---------- 签名 ----------
def sign_message(message: bytes, priv_key):
    signature = priv_key.sign(message, ec.ECDSA(hashes.SHA256()))
    return signature


# ---------- 验证 ----------
def verify_signature(message: bytes, signature: bytes, pub_key):
    try:
        pub_key.verify(signature, message, ec.ECDSA(hashes.SHA256()))
        return True
    except Exception:
        return False


# ---------- 批量生成身份 ----------
def generate_multiple_identities(n):
    """
    返回：
        identities: [ { "did":..., "priv":..., "pub":... }, ... ]
        did_pub_map: { did -> pub_key }
    """
    identities = []
    did_pub = {}

    for _ in range(n):
        priv, pub, did = generate_did_keypair()
        identities.append({
            "did": did,
            "priv": priv,
            "pub": pub
        })
        did_pub[did] = pub

    return identities, did_pub
