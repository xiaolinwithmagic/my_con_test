# block.py (修正版)
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
        view: int = 0,
        timestamp: Optional[float] = None  # 添加可选的时间戳参数
    ):
        self.parent_hash = parent_hash  # 字节类型，与QC一致
        self.height = height
        self.proposer = proposer
        self.payload = payload
        self.qc = qc  # 指向父区块的QC证明
        self.timestamp = timestamp if timestamp is not None else time.time()
        self.view = view
        
        # 计算区块哈希（字节类型）
        self.hash = self._compute_hash()
        # 保留十六进制字符串ID用于显示和调试
        self.id = self.hash.hex() if self.hash else None
        
    def _compute_hash(self) -> bytes:
        """
        计算区块哈希 - 私有方法，只在初始化时调用
        必须确保序列化的方式与 to_dict() 完全一致
        """
        # 使用 to_dict 方法获取序列化数据，但排除 hash 字段
        data = self._get_serializable_data()
        
        # 稳定序列化：按key排序确保确定性
        block_string = json.dumps(data, sort_keys=True, separators=(',', ':'))
        return hashlib.sha256(block_string.encode()).digest()
    
    def _get_serializable_data(self) -> Dict[str, Any]:
        """获取用于哈希计算的序列化数据"""
        return {
            "parent_hash": self.parent_hash.hex() if self.parent_hash else None,
            "height": self.height,
            "proposer": self.proposer,
            "payload": self.payload,
            "qc": self.qc.to_dict() if self.qc else None,
            "timestamp": self.timestamp,
            "view": self.view,
        }
    
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
        """从字典反序列化 - 修正版"""
        try:
            # 处理parent_hash
            parent_hash = None
            if data.get("parent_hash"):
                parent_hash = bytes.fromhex(data["parent_hash"])
            
            # 处理QC
            qc = None
            if data.get("qc"):
                try:
                    qc = QC.from_dict(data["qc"])
                except Exception as e:
                    raise ValueError(f"Failed to deserialize QC: {e}")
            
            # 从数据中获取时间戳
            timestamp = data.get("timestamp")
            if timestamp is None:
                timestamp = time.time()  # 如果没有时间戳，使用当前时间
            
            # 创建Block实例
            block = cls(
                parent_hash=parent_hash,
                height=data["height"],
                proposer=data["proposer"],
                payload=data["payload"],
                qc=qc,
                view=data.get("view", 0),
                timestamp=timestamp  # 传递时间戳
            )
            
            # 获取数据中的哈希
            expected_hash_hex = data.get("hash")
            if expected_hash_hex:
                expected_hash = bytes.fromhex(expected_hash_hex)
                
                # 验证哈希是否匹配
                if block.hash != expected_hash:
                    # 重新计算哈希以调试
                    computed_hash = block._compute_hash()
                    raise ValueError(
                        f"Block hash mismatch during deserialization:\n"
                        f"  Expected: {expected_hash.hex()[:16]}\n"
                        f"  Computed: {computed_hash.hex()[:16]}\n"
                        f"  Block data: height={block.height}, view={block.view}, proposer={block.proposer}"
                    )
            
            return block
            
        except Exception as e:
            raise ValueError(f"Failed to deserialize block: {e}")
    
    def __repr__(self):
        hash_str = self.hash.hex()[:8] if self.hash else "None"
        return f"Block(hash={hash_str}, height={self.height}, proposer={self.proposer}, view={self.view})"

# 辅助函数：创建创世区块
def create_genesis_block() -> Block:
    """创建创世区块（没有父哈希和QC）"""
    return Block(
        parent_hash=None,
        height=0,
        proposer="genesis",
        payload=["Genesis block"],
        qc=None,
        view=0,
        timestamp=time.time()  # 显式设置时间戳
    )