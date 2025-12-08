# crypto.py
from secrets import randbelow
from py_ecc.bls import G2ProofOfPossession as bls
from py_ecc.optimized_bn128 import curve_order


class BLS:
    """BLS 签名工具类"""

    @staticmethod
    def generate_keypair():
        """
        生成 BLS 密钥对
        返回: (sk: int, pk: bytes)
        """
        sk = randbelow(curve_order)
        pk = bls.SkToPk(sk)
        return sk, pk

    @staticmethod
    def sign(sk, message):
        """
        BLS 签名
        message: str 或 bytes
        返回: signature bytes
        """
        if isinstance(message, str):
            message = message.encode()
        return bls.Sign(sk, message)

    @staticmethod
    def verify(pk, message, signature):
        """
        验证 BLS 签名
        message: str 或 bytes
        signature: bytes
        返回: bool
        """
        if isinstance(message, str):
            message = message.encode()
        return bls.Verify(pk, message, signature)

    @staticmethod
    def aggregate(signatures):
        """
        聚合多个签名
        signatures: list[bytes]
        返回: bytes
        """
        return bls.Aggregate(signatures)

    @staticmethod
    def verify_aggregate(pks, messages, agg_sig):
        """
        验证聚合签名
        pks: list[bytes]
        messages: list[str/bytes] 或单条 str/bytes
        agg_sig: bytes
        返回: bool

        支持单条消息自动复制给每个公钥
        """
        # 如果 messages 不是 list/tuple -> 复制单条消息
        if not isinstance(messages, (list, tuple)):
            msg = messages
            if isinstance(msg, str):
                msg = msg.encode()
            elif isinstance(msg, (bytes, bytearray)):
                msg = bytes(msg)
            else:
                msg = bytes(msg)
            msgs = [msg] * len(pks)
        else:
            # messages 是可迭代对象 -> 逐一转换为 bytes
            msgs = []
            for m in messages:
                if isinstance(m, str):
                    msgs.append(m.encode())
                elif isinstance(m, (bytes, bytearray)):
                    msgs.append(bytes(m))
                else:
                    msgs.append(bytes(m))

        return bls.AggregateVerify(pks, msgs, agg_sig)


# ----------------------------
# 兼容旧接口
# ----------------------------
def sign(sk, message):
    """兼容旧接口：sign(sk, message)"""
    return BLS.sign(sk, message)


def aggregate(signatures):
    """兼容旧接口：aggregate(signatures)"""
    return BLS.aggregate(signatures)


def verify_aggregate(pks, messages, agg_sig):
    """兼容旧接口：verify_aggregate(pks, messages, agg_sig)"""
    return BLS.verify_aggregate(pks, messages, agg_sig)
