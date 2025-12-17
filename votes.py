# votes.py
import hashlib
from typing import Optional, Tuple, List, Dict, Any
from qc import QC
import logging



logger = logging.getLogger(__name__)

class Vote:
    """
    投票类，表示一个副本对特定区块的投票。
    关键改动：投票现在需要包含partial signature和验证所需的所有信息。
    """
    def __init__(
        self, 
        voter_id: str,           # 投票者节点ID
        voter_index: int,        # 投票者索引（用于位图）
        block_hash: bytes,       # 区块哈希（字节）
        view: int,               # 投票的视图号
        partial_signature: bytes, # 部分签名（对 (view, block_hash) 的签名）
        high_qc: Optional[QC] = None  # 投票者所知的最新QC（可选，用于安全性证明）
    ):
        self.voter_id = voter_id
        self.voter_index = voter_index  # 新增：索引用于位图
        self.block_hash = block_hash
        self.view = view
        self.partial_signature = partial_signature
        self.high_qc = high_qc  # 携带已知的最新QC，用于防止分叉
        self.timestamp = None  # 可由网络层添加
        
        # 计算投票的哈希ID，用于唯一标识
        self.id = self._calculate_id()
    
    def _calculate_id(self) -> str:
        """计算投票的唯一ID"""
        data = (
            self.voter_id.encode() + 
            self.voter_index.to_bytes(4, 'big') + 
            self.block_hash + 
            self.view.to_bytes(8, 'big')
        )
        return hashlib.sha256(data).digest().hex()[:16]
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典，用于网络传输"""
        return {
            "voter_id": self.voter_id,
            "voter_index": self.voter_index,
            "block_hash": self.block_hash.hex() if self.block_hash else None,
            "view": self.view,
            "partial_signature": self.partial_signature.hex() if self.partial_signature else None,
            "high_qc": self.high_qc.to_dict() if self.high_qc else None,
            "timestamp": self.timestamp,
            "id": self.id,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Vote':
        """从字典反序列化"""
        logger.info("[votes]从字典反序列化")
        # 处理block_hash
        block_hash = None
        if data.get("block_hash"):
            block_hash = bytes.fromhex(data["block_hash"])

        if block_hash is None:
            logger.error("jiushizheli")
            return None
        
        # 处理partial_signature
        partial_sig = None
        if data.get("partial_signature"):
            partial_sig = bytes.fromhex(data["partial_signature"])
        
        # 处理high_qc
        high_qc = None
        if data.get("high_qc"):
            high_qc = QC.from_dict(data["high_qc"])
        
        # 创建Vote实例
        vote = cls(
            voter_id=data["voter_id"],
            voter_index=data["voter_index"],
            block_hash=block_hash,
            view=data["view"],
            partial_signature=partial_sig,
            high_qc=high_qc
        )
        
        vote.timestamp = data.get("timestamp")
        return vote
    
    def verify(self, spk_i: bytes, expected_view: int, expected_block_hash: bytes) -> Tuple[bool, str]:
        """
        验证投票的有效性
        返回：(是否有效, 错误信息)
        """
        # 检查视图一致性
        if self.view != expected_view:
            return False, f"View mismatch: vote.view={self.view}, expected={expected_view}"
        
        # 检查区块哈希一致性
        if self.block_hash != expected_block_hash:
            return False, f"Block hash mismatch"
        
        # 验证partial signature（需要实现BLS验证）
        # 这里假设有一个verify_partial_sig函数
        # if not verify_partial_sig(spk_i, self.partial_signature, (self.view, self.block_hash)):
        #     return False, "Partial signature verification failed"
        
        # 验证high_qc的有效性（如果存在）
        if self.high_qc:
            # 简单的view检查，确保high_qc.view < self.view
            if self.high_qc.view >= self.view:
                return False, f"high_qc.view ({self.high_qc.view}) must be < vote.view ({self.view})"
        
        return True, "OK"
    
    def get_sign_data(self) -> bytes:
        """获取用于签名的数据（与验证时一致）"""
        # 组合view和block_hash作为签名消息
        return self.view.to_bytes(8, 'big') + self.block_hash
    
    def __repr__(self):
        return f"Vote(id={self.id}, voter={self.voter_id[:8]}, index={self.voter_index}, view={self.view})"

def is_conflicting_qc(qc1: Optional[QC], qc2: Optional[QC]) -> bool:
    """
    检查两个QC是否冲突（相同view但不同区块）
    根据新的QC结构进行判断
    """
    if not qc1 or not qc2:
        return False
    
    # 相同视图但不同区块哈希 => 冲突
    if qc1.view == qc2.view and qc1.block_hash != qc2.block_hash:
        return True
    
    # 附加检查：如果两个QC有重叠的签名者且冲突，也算冲突
    # 但这在实际中很少见，因为一个诚实副本不会对同一view的两个不同区块投票
    overlapping_signers = qc1.signer_bitmap & qc2.signer_bitmap
    if overlapping_signers != 0 and qc1.block_hash != qc2.block_hash:
        # 有重叠的签名者对两个不同区块的同一view签名 => 严重冲突
        return True
    
    return False

def create_vote_from_block(voter_id: str, voter_index: int, block, view: int, 
                          partial_sig: bytes, high_qc: Optional[QC] = None) -> Vote:
    """从区块创建投票（便捷函数）"""
    return Vote(
        voter_id=voter_id,
        voter_index=voter_index,
        block_hash=block.hash,  # 使用区块的字节哈希
        view=view,
        partial_signature=partial_sig,
        high_qc=high_qc
    )

def count_votes(votes: List[Vote]) -> Tuple[int, int, int]:
    """
    统计投票集合
    返回：(总票数，唯一投票者数，有效票数)
    """
    total = len(votes)
    unique_voters = len(set(v.voter_id for v in votes))
    
    # 这里可以添加有效性检查逻辑
    valid_count = total  # 假设所有票都有效
    
    return total, unique_voters, valid_count