import logging
from typing import Tuple
from qc import QC
from merkle_utils import MerkleUtils
from qc import MerkleProof
import time

logger = logging.getLogger(__name__)


class QCVerifier:
    """统一的QC验证器"""
    
    def __init__(self, f: int, gpk: bytes = None):
        self.f = f
        self.gpk = gpk
    
    def verify(self, replica_node: any, qc: QC, require_full: bool = False) -> Tuple[bool, str]:
        """验证QC - 统一入口"""
        # 创世QC特殊处理
        if qc.view == 0:
            logger.info("验证创世QC（特殊处理）")
            return True, "创世QC"
        
        # 基础检查
        if qc.view < replica_node.state.locked_qc.view:
            return False, f"QC视图 {qc.view} 小于锁定视图 {replica_node.state.locked_qc.view}"
        
        # 签名者数量检查
        if qc.get_signer_count() < 2 * self.f + 1:
            return False, f"签名者不足: {qc.get_signer_count()}"
        
        # 聚合签名验证
        if self.gpk:
            return self._verify_aggregate_signature(qc)
        else:
            logger.warning("跳过签名验证（无组公钥）")
            return True, "跳过签名验证"
        
        # 如果需要完整验证，进行Merkle证明验证
        if require_full:
            return self.verify_with_merkle_proof(replica_node, qc)
    
    def _verify_aggregate_signature(self, qc: QC) -> Tuple[bool, str]:
        """验证聚合签名"""
        try:
            from crypto import BLS
            message = qc.view.to_bytes(8, 'big') + qc.block_hash
            if not BLS.verify_group_signature(self.gpk, message, qc.aggregate_signature):
                return False, "聚合签名验证失败"
            return True, "聚合签名验证成功"
        except Exception as e:
            return False, f"聚合签名验证出错: {e}"


    def verify_with_merkle_proof(self, qc: QC, replica_index: int) -> bool:
        """通过Merkle证明验证QC"""
        # 1. 向Leader请求证明
        leader_id = self.leader_for_view(qc.view)
        
        self.network.request_merkle_proof(
            requester_id=self.id,
            leader_id=leader_id,
            replica_index=replica_index,
            view=qc.view,
            block_hash=qc.block_hash
        )
        
        # 2. 等待响应（带超时）
        start_time = time.time()
        proof_received = False
        
        while time.time() - start_time < PROOF_REQUEST_TIMEOUT:
            # 检查是否收到证明（实际应该通过消息队列）
            proof_key = (qc.view, qc.block_hash, replica_index)
            if hasattr(self, 'cached_proofs') and proof_key in self.cached_proofs:
                proof = self.cached_proofs.pop(proof_key)
                
                # 3. 验证证明
                if self.verify_merkle_proof(proof, qc):
                    proof_received = True
                    break
            
            time.sleep(0.01)
        
        proof_received =True  # 临时 todo
        if not proof_received:
            logging.warning(f"[{self.id}] Merkle proof request timeout")
            # 可以尝试向其他副本请求
            return self.request_proof_from_witnesses(qc, replica_index)
        
        return proof_received
    
    def verify_merkle_proof(self, proof: MerkleProof, qc: QC) -> bool:
        """验证Merkle证明"""
        try:
            # 1. 验证部分签名
            logger.debug(f"[{self.id}] 111Verifying Merkle proof for replica {proof.replica_index}")
            sign_data = proof.view.to_bytes(8, 'big') + proof.block_hash
            # 需要获取该副本的spk_i todo
            # if not self.bls.verify_group_signature(self.gpk, proof.partial_signature, sign_data):
            #     logging.error(f"[{self.id}] Partial signature verification failed in Merkle proof")
            #     return False
            
            # 2. 验证Merkle路径
            leaf_hash = proof.calculate_leaf_hash()
            # 这里需要实现Merkle路径验证
            logging.debug(f"[{self.id}] Verifying Merkle proof for replica {proof.replica_index}")
            computed_root = self.compute_root_from_proof(leaf_hash, proof.sibling_hashes)
            if computed_root != qc.merkle_root:
                logging.error(f"[{self.id}] Merkle root mismatch: computed {computed_root.hex()[:8]} != qc {qc.merkle_root.hex()[:8]}")
                return False
            
            return True
        except Exception as e:
            logging.error(f"[{self.id}] Merkle proof verification error: {e}")
            return False
    
    def request_proof_from_witnesses(self, qc: QC, replica_index: int, k=2) -> bool:
        """向witness副本请求证明（fallback）"""
        # 找出可能存有证明的其他副本
        # 这里简化实现
        return False