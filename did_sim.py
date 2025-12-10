from hashlib import sha256
# 导入修正后的BLS类和序列化函数
from crypto import BLS

# -----------------------------
# 序列化 G2 公钥
# -----------------------------
def serialize_g2_point(g2_point):
    """
    将 G2 公钥点序列化为 bytes
    g2_point: tuple(FQ2, FQ2)
    """
    if isinstance(g2_point, tuple) and len(g2_point) == 2:
        x_bytes = int(g2_point[0][0]).to_bytes(32, "big") + int(g2_point[0][1]).to_bytes(32, "big")
        y_bytes = int(g2_point[1][0]).to_bytes(32, "big") + int(g2_point[1][1]).to_bytes(32, "big")
        return x_bytes + y_bytes
    else:
        raise ValueError("Invalid G2 point")

# -----------------------------
# 生成单个 DID
# -----------------------------
def generate_did_keypair():
    """
    返回: (priv, pub_bytes, did)
    DID = did:sim:sha256(pub_bytes)
    """
    priv, pub_bytes = BLS.generate_keypair()  # pub_bytes 已经是 bytes
    did = "did:sim:" + sha256(pub_bytes).hexdigest()
    return priv, pub_bytes, did


# -----------------------------
# 批量生成 DID
# -----------------------------
def generate_multiple_identities(n):
    identities = []
    did_pub_map = {}

    for _ in range(n):
        priv, pub_bytes, did = generate_did_keypair()
        identities.append({
            "did": did,
            "priv": priv,
            "pub": pub_bytes
        })
        did_pub_map[did] = pub_bytes

    return identities, did_pub_map


# -----------------------------
# 消息签名与验证
# -----------------------------
def sign_message(message: bytes, priv_key):
    return BLS.sign(priv_key, message)

def verify_signature(message: bytes, signature, pub_bytes):
    return BLS.verify(pub_bytes, message, signature)
