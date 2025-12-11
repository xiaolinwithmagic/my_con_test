# did_bls_demo.py

from secrets import randbelow
from hashlib import sha256
from py_ecc.bls import G2ProofOfPossession as bls
from py_ecc.optimized_bn128 import curve_order
import logging

# 创建当前模块的logger实例（推荐方式，而非直接用logging.root）
logger = logging.getLogger(__name__)

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
#     logger.info(f"Generated DID: {did}")

#     # 测试签名与验证
#     message = b"Hello, BLS!"
#     signature = sign_message(message, priv)
#     logger.info(f"Signature (hex): {signature.hex()}")

#     valid = verify_signature(message, signature, pub)
#     logger.info(f"Signature valid: {valid}")

#     # 批量生成
#     identities, did_pub_map = generate_multiple_identities(3)
#     logger.info("\n=== Batch Generated Identities ===")
#     for idx, identity in enumerate(identities):
#         logger.info(f"Identity {idx+1}: DID={identity['did'][:20]}...")
#     logger.info(f"\nDID to pubkey map length: {len(did_pub_map)}")


def test_bls_signature():
    """测试BLS签名功能"""
    from crypto import BLS
    
    print("=== 测试BLS签名 ===")
    
    # 生成密钥对
    sk, pk = BLS.generate_keypair()
    print(f"私钥类型: {type(sk)}")
    print(f"公钥类型: {type(pk)}, 长度: {len(pk)}")
    
    # 签名
    message = b"test message"
    signature = BLS.sign(sk, message)
    print(f"签名类型: {type(signature)}, 长度: {len(signature)}")
    
    # 验证
    is_valid = BLS.verify(pk, message, signature)
    print(f"单个签名验证: {is_valid}")
    
    # 测试聚合签名
    print("\n=== 测试聚合签名 ===")
    sks, pks, group_pk = BLS.generate_partial_keys(3)
    
    # 三个部分签名
    sigs = []
    for i in range(3):
        sig = BLS.sign(sks[i], message)
        sigs.append(sig)
        # 验证部分签名
        if not BLS.verify(pks[i], message, sig):
            print(f"部分签名 {i} 验证失败!")
            return False
    
    # 聚合签名
    agg_sig = BLS.aggregate_partial_sigs(sigs)
    print(f"聚合签名长度: {len(agg_sig)}")
    
    # 验证聚合签名
    is_valid = BLS.verify_group_signature(group_pk, message, agg_sig)
    print(f"聚合签名验证: {is_valid}")
    
    return is_valid


if __name__ == "__main__":
    test_bls_signature()