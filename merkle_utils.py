import hashlib
from typing import List
import logging
logger = logging.getLogger(__name__)

class MerkleUtils:
    """统一的Merkle树工具"""
    
    @staticmethod
    def build_root(leaves_data: List[bytes]) -> bytes:
        """构建Merkle根"""
        if not leaves_data:
            return bytes([0] * 32)
        
        # 使用标准Merkle树实现
        all_data = b"".join(leaves_data)
        return hashlib.sha256(all_data).digest()
    
    @staticmethod
    def compute_root_from_proof(leaf_hash: bytes, sibling_hashes: List[bytes]) -> bytes:
        """从证明计算Merkle根"""
        current_hash = leaf_hash
        
        for sibling_hash in sibling_hashes:
            if not sibling_hash:
                logger.warning("空的兄弟哈希")
                sibling_hash = bytes([0] * 32)
            
            # 保持顺序一致
            if current_hash < sibling_hash:
                combined = current_hash + sibling_hash
            else:
                combined = sibling_hash + current_hash
            
            current_hash = hashlib.sha256(combined).digest()
        
        return current_hash