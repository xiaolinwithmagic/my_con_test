import logging
from typing import Dict, Tuple
from block import Block
from qc import QC
logger = logging.getLogger(__name__)


class ProposalValidator:
    """统一的提案验证器"""
    
    def __init__(self, f: int, nodes: Dict[str, any], genesis_block: Block):
        self.f = f
        self.nodes = nodes
        self.genesis_block = genesis_block
    
    def validate(self, node: any, block: Block, proposal_qc: QC, view: int) -> Tuple[bool, str]:
        """验证提案 - 合并所有验证逻辑"""
        # 基础结构验证
        if not block.validate():
            return False, "区块验证失败"
        
        # 提案者验证
        leader_id = self._leader_for_view(view)
        if block.proposer != leader_id:
            return False, f"提案者 {block.proposer} 不是当前Leader {leader_id}"
        
        # 视图号验证
        if block.view != view:
            return False, f"区块视图 {block.view} 不匹配当前视图 {view}"
        
        # 高度连续性验证
        if block.height <= node.state.height:
            logger.warning(f"区块高度 {block.height} 未高于当前高度 {node.state.height}")
        else:
            logger.info(f"区块高度 {block.height} 高于当前高度 {node.state.height}，继续验证")
        
        # 父区块验证
        is_valid, reason = self._validate_parent(block, node)
        if not is_valid:
            return False, reason
        
        # QC验证（如果存在）
        if proposal_qc:
            qc_valid, qc_reason = self._validate_qc_for_proposal(proposal_qc, block)
            if not qc_valid:
                return False, qc_reason
        
        return True, "验证通过"
    
    def _validate_parent(self, block: Block, node: any) -> Tuple[bool, str]:
        """验证父区块"""
        if block.height == 1:
            # 创世区块子区块
            if block.parent_hash != self.genesis_block.hash:
                return False, "父哈希不匹配创世区块哈希"
            return True, "OK - 创世区块子区块"
        
        # 普通区块
        parent_block = node.state.get_block_by_hash(block.parent_hash)
        if not parent_block:
            return False, "父区块未找到"
        
        if block.height != parent_block.height + 1:
            return False, "区块高度不连续"
        
        return True, "父区块验证通过"
    
    def _validate_qc_for_proposal(self, qc: QC, block: Block) -> Tuple[bool, str]:
        """验证提案中的QC"""
        # QC应证明父区块
        if qc.block_hash != block.parent_hash:
            return False, "QC不匹配区块的父区块"
        return True, "QC验证通过"
    
    def _leader_for_view(self, view: int) -> str:
        """计算Leader"""
        ids = sorted(self.nodes.keys())
        return ids[view % len(ids)] if ids else ""