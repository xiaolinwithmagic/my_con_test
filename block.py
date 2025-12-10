# block.py (修改后)
import hashlib
import json
import time
from typing import Optional, List, Dict, Any
from qc import QC  # 导入新的QC类

class Block:
    def __init__(
        self, 
        parent_hash: Optional[bytes],  # 改为字节类型的父哈希
        height: int, 
        proposer: str, 
        payload: List[Any], 
        qc: Optional[QC] = None,  # 新的QC对象
        view: int = 0
    ):
        self.parent_hash = parent_hash  # 字节类型，与QC一致
        self.height = height
        self.proposer = proposer
        self.payload = payload
        self.qc = qc  # 指向父区块的QC证明
        self.timestamp = time.time()
        self.view = view
        # 计算区块哈希（字节类型）
        self.hash = self.calculate_hash()
        # 保留十六进制字符串ID用于显示和调试
        self.id = self.hash.hex() if self.hash else None
        
    def calculate_hash(self) -> bytes:
        """
        计算区块哈希，必须与QC中使用的block_hash一致。
        哈希计算需要包含所有关键字段，特别是QC的merkle_root。
        """
        # 序列化所有关键数据
        block_data = {
            "parent_hash": self.parent_hash.hex() if self.parent_hash else None,
            "height": self.height,
            "proposer": self.proposer,
            "payload": self.payload,
            # QC信息：包含merkle_root确保绑定
            "qc_view": self.qc.view if self.qc else None,
            "qc_block_hash": self.qc.block_hash.hex() if self.qc and self.qc.block_hash else None,
            "qc_merkle_root": self.qc.merkle_root.hex() if self.qc and self.qc.merkle_root else None,
            "timestamp": int(self.timestamp * 1000),  # 毫秒精度，避免浮点数问题
            "view": self.view,
        }
        
        # 稳定序列化：按key排序确保确定性
        block_string = json.dumps(block_data, sort_keys=True, separators=(',', ':'))
        return hashlib.sha256(block_string.encode()).digest()  # 返回字节
    
    def validate(self) -> bool:
        """基本验证逻辑"""
        if self.height < 0:
            return False
        if not self.proposer:
            return False
        if self.parent_hash and len(self.parent_hash) != 32:  # SHA256哈希长度
            return False
        # 验证QC的存在性（创世块除外）
        if self.height > 0 and not self.qc:
            return False
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典，用于网络传输或存储"""
        return {
            "hash": self.hash.hex() if self.hash else None,
            "id": self.id,  # 十六进制字符串
            "parent_hash": self.parent_hash.hex() if self.parent_hash else None,
            "height": self.height,
            "proposer": self.proposer,
            "payload": self.payload,
            "qc": self.qc.to_dict() if self.qc else None,
            "timestamp": self.timestamp,
            "view": self.view,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Block':
        """从字典反序列化"""
        # 处理parent_hash
        parent_hash = None
        if data.get("parent_hash"):
            parent_hash = bytes.fromhex(data["parent_hash"])
        
        # 处理QC
        qc = None
        if data.get("qc"):
            qc = QC.from_dict(data["qc"])
        
        # 创建Block实例
        block = cls(
            parent_hash=parent_hash,
            height=data["height"],
            proposer=data["proposer"],
            payload=data["payload"],
            qc=qc,
            view=data.get("view", 0)
        )
        
        # 验证反序列化的哈希是否一致
        expected_hash = bytes.fromhex(data["hash"]) if data.get("hash") else None
        if expected_hash and block.hash != expected_hash:
            raise ValueError("Block hash mismatch during deserialization")
            
        return block
    
    def __repr__(self):
        return f"Block(hash={self.hash.hex()[:8]}..., height={self.height}, proposer={self.proposer}, view={self.view})"

# 辅助函数：创建创世区块
def create_genesis_block() -> Block:
    """创建创世区块（没有父哈希和QC）"""
    return Block(
        parent_hash=None,
        height=0,
        proposer="genesis",
        payload=["Genesis block"],
        qc=None,
        view=0
    )