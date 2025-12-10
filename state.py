# state.py (修改后)
import logging
from typing import Dict, Optional
from qc import QC
from block import Block

logging.basicConfig(level=logging.DEBUG)

# 删除旧的 DummyQC 类，用实际的创世 QC 替代

class NodeState:
    def __init__(self, node_id: str, f: int = 1):
        self.node_id = node_id
        self.f = f
        self.f_plus_1 = f + 1
        self.full_q_threshold = 2 * f + 1
        
        # 创世QC - 使用新的QC结构
        genesis_qc = self._create_genesis_qc()
        
        # 状态变量
        self.locked_qc = genesis_qc          # 已锁定的最高QC
        self.latest_qc = genesis_qc          # 收到的最新QC
        self.commit_qc = genesis_qc          # 已提交的最高QC
        self.voted_blocks = set()            # 已投票的区块ID（十六进制字符串）
        self.block_tree: Dict[str, Block] = {}  # 区块ID -> Block对象
        
        # 存储自己的partial签名 { (view, block_hash_hex): partial_sig }
        self.my_partial_sigs: Dict[tuple, bytes] = {}
        
        # 缓存已验证的Merkle证明 { (view, block_hash_hex, index): MerkleProof }
        self.merkle_proof_cache: Dict[tuple, bytes] = {}
        
        # 跟踪怀疑的QC，用于触发额外验证
        self.suspicious_qc_count = 0
        
    def _create_genesis_qc(self) -> QC:
        """创建创世QC（特殊值）"""
        # 创世区块哈希可以是全0或特定值
        genesis_block_hash = bytes([0] * 32)
        
        return QC(
            view=0,
            block_hash=genesis_block_hash,
            aggregate_signature=b"",  # 空签名
            signer_bitmap=0,          # 无签名者
            merkle_root=bytes([0] * 32)  # 空的Merkle根
        )
    
    def update_locked_qc(self, qc: QC) -> bool:
        """更新locked_qc：只有当新QC的view更高时才更新"""
        if qc and qc.view > self.locked_qc.view:
            logging.debug(f"[{self.node_id}] update_locked_qc -> view={qc.view}, block={qc.block_hash.hex()[:8]}")
            self.locked_qc = qc
            
            # 安全规则：locked_qc更新后，需要重新评估哪些区块可以提交
            self._try_commit_blocks()
            return True
        return False
    
    def update_latest_qc(self, qc: QC) -> bool:
        """更新latest_qc：只要新QC的view更高就更新"""
        if qc and qc.view > self.latest_qc.view:
            logging.debug(f"[{self.node_id}] update_latest_qc -> view={qc.view}, block={qc.block_hash.hex()[:8]}")
            self.latest_qc = qc
            return True
        return False
    
    def update_commit_qc(self, qc: QC) -> bool:
        """更新commit_qc：只有当新QC的view更高且对应区块已提交时才更新"""
        if qc and qc.view > self.commit_qc.view:
            # 验证对应区块是否在block_tree中且高度连续
            block = self.get_block_by_hash(qc.block_hash)
            if block and self._is_block_committable(block):
                logging.debug(f"[{self.node_id}] update_commit_qc -> view={qc.view}, block={qc.block_hash.hex()[:8]}")
                self.commit_qc = qc
                return True
        return False
    
    def add_block(self, block: Block) -> bool:
        """添加区块到block_tree"""
        if block.id not in self.block_tree:
            self.block_tree[block.id] = block
            logging.debug(f"[{self.node_id}] add_block -> id={block.id[:8]}, height={block.height}, view={block.view}")
            
            # 新区块添加后，尝试提交连续的区块
            self._try_commit_blocks()
            return True
        return False
    
    def get_block(self, block_id: str) -> Optional[Block]:
        """通过区块ID（十六进制字符串）获取区块"""
        return self.block_tree.get(block_id)
    
    def get_block_by_hash(self, block_hash: bytes) -> Optional[Block]:
        """通过区块哈希（字节）获取区块"""
        block_hash_hex = block_hash.hex()
        for block in self.block_tree.values():
            if block.hash == block_hash or block.id == block_hash_hex:
                return block
        return None
    
    def has_voted(self, block_id: str) -> bool:
        """检查是否已对该区块投票"""
        return block_id in self.voted_blocks
    
    def record_vote(self, block_id: str, view: int, partial_sig: Optional[bytes] = None) -> None:
        """记录投票，并可选保存自己的partial签名"""
        logging.debug(f"[{self.node_id}] record_vote for block={block_id[:8]}, view={view}")
        self.voted_blocks.add(block_id)
        
        # 保存自己的partial签名，用于后续验证
        if partial_sig:
            block = self.get_block(block_id)
            if block:
                key = (view, block.hash)
                self.my_partial_sigs[key] = partial_sig
    
    def record_partial_sig(self, view: int, block_hash: bytes, partial_sig: bytes) -> None:
        """记录自己生成的partial签名"""
        key = (view, block_hash)
        self.my_partial_sigs[key] = partial_sig
        logging.debug(f"[{self.node_id}] recorded partial_sig for view={view}, block={block_hash.hex()[:8]}")
    
    def get_my_partial_sig(self, view: int, block_hash: bytes) -> Optional[bytes]:
        """获取自己生成的partial签名"""
        return self.my_partial_sigs.get((view, block_hash))
    
    def _try_commit_blocks(self) -> None:
        """尝试提交连续的区块：从commit_qc开始，提交其后连续的两个子区块"""
        # 获取当前commit_qc对应的区块
        committed_block = self.get_block_by_hash(self.commit_qc.block_hash)
        if not committed_block:
            return
        
        # 查找连续的区块链
        current = committed_block
        blocks_to_commit = []
        
        # 提交规则：需要连续的两个区块（B1, B2）且B2有QC
        for _ in range(2):
            # 查找current的子区块
            children = [b for b in self.block_tree.values() if b.parent_hash == current.hash]
            if not children:
                break
            
            # 选择view最高的子区块（假设没有分叉）
            child = max(children, key=lambda b: b.view)
            blocks_to_commit.append(child)
            current = child
        
        # 如果找到连续的两个新区块，提交它们
        if len(blocks_to_commit) >= 2:
            for block in blocks_to_commit:
                if block.height > committed_block.height:
                    self._commit_block(block)
    
    def _commit_block(self, block: Block) -> None:
        """提交区块（实际应用交易）"""
        logging.info(f"[{self.node_id}] COMMIT block={block.id[:8]}, height={block.height}, view={block.view}")
        
        # 这里可以添加实际应用交易的逻辑
        # 例如：apply_transactions(block.payload)
        
        # 更新commit_qc为区块的QC
        if block.qc:
            self.update_commit_qc(block.qc)
    
    def _is_block_committable(self, block: Block) -> bool:
        """检查区块是否可提交：需要父区块已提交"""
        if block.height == 0:  # 创世区块
            return True
        
        # 获取父区块
        parent = self.get_block_by_hash(block.parent_hash)
        if not parent:
            return False
        
        # 检查父区块是否已提交
        parent_committed = (self.commit_qc.block_hash == parent.hash)
        return parent_committed
    
    def mark_qc_suspicious(self, qc: QC, reason: str = "") -> None:
        """标记QC为可疑，用于触发额外验证"""
        self.suspicious_qc_count += 1
        logging.warning(f"[{self.node_id}] Suspicious QC detected: view={qc.view}, reason={reason}")
        
        # 如果连续多次可疑，可能需要触发view change
        if self.suspicious_qc_count >= 3:
            logging.error(f"[{self.node_id}] Too many suspicious QCs,可能需要触发view change")
    
    def reset_suspicion_counter(self) -> None:
        """重置可疑计数器"""
        self.suspicious_qc_count = 0
    
    @property
    def height(self) -> int:
        """当前最高区块高度"""
        if not self.block_tree:
            return 0
        return max(block.height for block in self.block_tree.values())
    
    def get_chain_head(self) -> Optional[Block]:
        """获取链头（最高且view最大的区块）"""
        if not self.block_tree:
            return None
        
        # 首先按高度排序，然后按view排序
        max_height = self.height
        highest_blocks = [b for b in self.block_tree.values() if b.height == max_height]
        
        if not highest_blocks:
            return None
        
        return max(highest_blocks, key=lambda b: b.view)