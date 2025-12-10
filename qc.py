from dataclasses import dataclass
from typing import List, Optional
import hashlib

@dataclass
class QC:
    """
    新的QC数据结构，集成可验证的安全特性。
    关键改动：
    1. 用 signer_bitmap（整数位图）替代原来的 signers 列表，节省空间并支持O(1)快速检查。
    2. 增加 merkle_root 字段，用于后续按需请求Merkle证明来验证partial sig的合法性。
    3. 不再在QC中直接存储 signatures 列表，该列表仅由Leader在本地存储以生成证明。
    """
    view: int                    # QC对应的视图编号，用于检测view篡改
    block_hash: bytes            # 区块哈希，更明确的命名
    aggregate_signature: bytes   # 聚合签名 (原 agg 字段)
    signer_bitmap: int           # 签名者位图，第i位为1表示索引i的副本参与了签名
    merkle_root: bytes           # 所有partial signature的Merkle树根哈希
    
    # 可选：用于调试或兼容性的字段，正式版本可考虑移除
    # signatures: Optional[List[bytes]] = None  # 不再通过网络发送，Leader本地存储
    
    def get_signer_count(self) -> int:
        """计算位图中1的个数，即参与签名的副本数。用于快速法定人数检查。"""
        return bin(self.signer_bitmap).count("1")
    
    def is_signer(self, index: int) -> bool:
        """快速检查指定索引的副本是否在签名者集合中。"""
        if index < 0:
            return False
        return (self.signer_bitmap >> index) & 1 == 1
    
    def to_dict(self) -> dict:
        """转换为字典，用于序列化传输或存储。"""
        return {
            "view": self.view,
            "block_hash": self.block_hash.hex() if self.block_hash else None,
            "aggregate_signature": self.aggregate_signature.hex() if self.aggregate_signature else None,
            "signer_bitmap": self.signer_bitmap,
            "merkle_root": self.merkle_root.hex() if self.merkle_root else None,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "QC":
        """从字典反序列化。"""
        return cls(
            view=data["view"],
            block_hash=bytes.fromhex(data["block_hash"]) if data["block_hash"] else None,
            aggregate_signature=bytes.fromhex(data["aggregate_signature"]) if data["aggregate_signature"] else None,
            signer_bitmap=data["signer_bitmap"],
            merkle_root=bytes.fromhex(data["merkle_root"]) if data["merkle_root"] else None,
        )

@dataclass
class MerkleProof:
    """
    按需请求的Merkle证明，用于证明某个partial signature确实被包含在QC的Merkle树中。
    当副本对QC有怀疑（例如自己不在signer_bitmap中，或view跳跃过大）时，向Leader请求此证明。
    """
    replica_index: int           # 要证明的副本索引
    partial_signature: bytes     # 该副本的部分签名
    view: int                    # 此签名对应的视图（必须与QC中的view一致）
    block_hash: bytes            # 此签名对应的区块哈希
    sibling_hashes: List[bytes]  # Merkle路径上的兄弟节点哈希列表
    
    def calculate_leaf_hash(self) -> bytes:
        """计算该叶节点的哈希值，计算方式必须与Leader构造Merkle树时完全一致。"""
        # 将数据序列化：索引 + 部分签名 + 视图 + 区块哈希
        # 注意：这个顺序必须与Leader端构建Merkle树时的顺序严格一致
        data = (
            self.replica_index.to_bytes(4, 'big') + 
            self.partial_signature + 
            self.view.to_bytes(8, 'big') + 
            self.block_hash
        )
        return hashlib.sha256(data).digest()
    
    def to_dict(self) -> dict:
        """序列化"""
        return {
            "replica_index": self.replica_index,
            "partial_signature": self.partial_signature.hex(),
            "view": self.view,
            "block_hash": self.block_hash.hex(),
            "sibling_hashes": [h.hex() for h in self.sibling_hashes],
        }
    def calculate_leaf_hash(self) -> bytes:
        """计算当前证明对应的叶子节点哈希（与Leader构建Merkle树时的叶子哈希逻辑一致）"""
        leaf_data = (
            self.replica_index.to_bytes(4, 'big') +
            self.partial_signature +
            self.view.to_bytes(8, 'big') +
            self.block_hash
        )
        return hashlib.sha256(leaf_data).digest()
    
    @classmethod
    def from_dict(cls, data: dict) -> "MerkleProof":
        """反序列化"""
        return cls(
            replica_index=data["replica_index"],
            partial_signature=bytes.fromhex(data["partial_signature"]),
            view=data["view"],
            block_hash=bytes.fromhex(data["block_hash"]),
            sibling_hashes=[bytes.fromhex(h) for h in data["sibling_hashes"]],
        )

# 提示：原有的 PartialQC 类在新的安全方案中可能不再需要。
# 因为其功能（携带聚合签名和签名者数量）已被新的QC类覆盖。
# 如果其他代码依赖它，可以暂时保留但标记为弃用。