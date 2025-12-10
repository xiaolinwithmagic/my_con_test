# did_bls_demo.py

from secrets import randbelow
from hashlib import sha256
from py_ecc.bls import G2ProofOfPossession as bls
from py_ecc.optimized_bn128 import curve_order

# -----------------------------
# BLS 工具类
# -----------------------------
class BLS:
    @staticmethod
    def generate_keypair():
        """生成单个 BLS 密钥对"""
        sk = randbelow(curve_order)
        pk = bls.SkToPk(sk)
        return sk, pk

    @staticmethod
    def generate_single_keypair():
        """兼容函数"""
        return BLS.generate_keypair()

    @staticmethod
    def sign(sk, message: bytes):
        return bls.Sign(sk, message)

    @staticmethod
    def verify(pk, message: bytes, signature):
        return bls.Verify(pk, message, signature)


    @staticmethod
    def generate_partial_keys(n):
        """
        模拟分布式密钥生成
        返回：sks, pks, group_pk
        """
        sks = []
        pks = []
        for _ in range(n):
            sk = randbelow(curve_order)
            pk = bls.SkToPk(sk)
            sks.append(sk)
            pks.append(pk)

        # 聚合成 group_pk
        group_pk = bls._AggregatePKs(pks)
        return sks, pks, group_pk

    @staticmethod
    def sign_partial(sk_i, message):
        if isinstance(message, str):
            message = message.encode()
        return bls.Sign(sk_i, message)

    @staticmethod
    def aggregate_partial_sigs(partial_sigs):
        return bls.Aggregate(partial_sigs)

    @staticmethod
    def verify_group_signature(group_pk, message, agg_sig):
        if isinstance(message, str):
            message = message.encode()
        return bls.Verify(group_pk, message, agg_sig)



# # -----------------------------
# # 测试
# # -----------------------------
# if __name__ == "__main__":
#     # 单个 DID
#     priv, pub, did = generate_did_keypair()
#     print(f"Generated DID: {did}")

#     # 测试签名与验证
#     message = b"Hello, BLS!"
#     signature = sign_message(message, priv)
#     print(f"Signature (hex): {signature.hex()}")

#     valid = verify_signature(message, signature, pub)
#     print(f"Signature valid: {valid}")

#     # 批量生成
#     identities, did_pub_map = generate_multiple_identities(3)
#     print("\n=== Batch Generated Identities ===")
#     for idx, identity in enumerate(identities):
#         print(f"Identity {idx+1}: DID={identity['did'][:20]}...")
#     print(f"\nDID to pubkey map length: {len(did_pub_map)}")