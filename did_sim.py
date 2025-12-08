from hashlib import sha256
from crypto import BLS  # 导入 BLS 类

# ---------- 生成 DID 身份 ----------
def generate_did_keypair():
    """
    生成 (private_key, public_key, did)
    DID = did:sim:sha256(pubkey_bytes)
    """
    # 调用 crypto.py 中的 BLS.generate_keypair 方法
    priv, pub = BLS.generate_keypair()

    # 计算 DID
    did = "did:sim:" + sha256(pub).hexdigest()
    return priv, pub, did

# ---------- 批量生成身份 ----------
def generate_multiple_identities(n):
    """
    批量生成 n 个 DID 身份
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

# ---------- 签名 ----------
def sign_message(message: bytes, priv_key):
    """
    使用 BLS 私钥对消息进行签名
    """
    return BLS.sign(priv_key, message)

# ---------- 验证 ----------
def verify_signature(message: bytes, signature, pub_key):
    """
    使用 BLS 公钥验证签名
    """
    return BLS.verify(pub_key, message, signature)

# ---------- 测试 ----------
if __name__ == "__main__":
    # 测试生成 DID 密钥对
    priv, pub, did = generate_did_keypair()
    print(f"Generated DID: {did}")

    # 测试签名和验证
    message = b"Hello, BLS!"
    signature = sign_message(message, priv)
    print(f"Signature: {signature}")

    is_valid = verify_signature(message, signature, pub)
    print(f"Signature valid: {is_valid}")

    # 测试批量生成身份
    identities, did_pub_map = generate_multiple_identities(3)
    print(f"Generated identities: {identities}")
    print(f"DID to public key map: {did_pub_map}")