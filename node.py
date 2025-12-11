# node.py (关键修复部分)
import logging
import threading
import hashlib
import json
import time
from typing import Dict, List, Optional, Tuple, Any
from state import NodeState
from votes import Vote, is_conflicting_qc
from qc import QC, MerkleProof
from crypto import BLS
from world_state import WorldState
from metrics import Metrics
from network import Network

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

SOFT_VOTE_TIMEOUT = 0.6
DEFER_TIMEOUT = 1.2
VIEW_TIMEOUT = 3.0
PROOF_REQUEST_TIMEOUT = 1.0

class Node:
    def __init__(self, node_id, index, priv_key, pub_key, network: Network,group_pk,did_priv: int, did_pub, f=1, all_nodes=None):
        self.id = node_id
        self.index = index  # 新增：节点索引（用于signer_bitmap）
        self.priv = priv_key
        self.pub = pub_key
        self.network = network
        self.f = f
        self.gpk = group_pk
        self.did_priv = did_priv
        self.did_pub = did_pub
        
        # 状态管理
        self.state = NodeState(node_id=node_id, f=f)
        self.view = 0
        self.is_leader = False
        self.all_nodes = all_nodes or []

         # 公钥查找表
        self.did_pub_lookup: Dict[str, Any] = {}  # DID -> 公钥
        self.consensus_pub_lookup: Dict[str, Any] = {}  # 节点ID -> 共识公钥
        
        
        # 密码学组件
        self.bls = BLS()  # 假设BLS有适当的初始化
        
        # 世界状态和监控
        self.world_state = WorldState()
        self.metrics = Metrics() if Metrics else None
        
        # 消息存储（按新的方案简化）
        self.pending_votes = {}  # {block_id: List[Vote]}
        self.pending_proof_requests = {}  # {request_id: (timestamp, callback)}
        
        # 定时器
        self._soft_timer = None
        self._defer_timer = None
        self._view_timer = None
        
        # 当前处理中的提案
        self.current_proposal = None
        
        # 注册到网络
        # self.network.register(self)
    
    # ==================== 消息处理（更新） ====================
    
    def receive_message(self, msg_type, payload):
        """处理所有类型的消息"""
        sender = payload.get("sender", "unknown")
        
        if msg_type == self.network.MSG_TYPES["PROPOSAL"]:
            self.handle_proposal(payload)
        elif msg_type == self.network.MSG_TYPES["VOTE"]:
            self.handle_vote(payload)
        elif msg_type == self.network.MSG_TYPES["NEW_VIEW"]:
            self.handle_new_view(payload)
        elif msg_type == self.network.MSG_TYPES["REQUEST_MERKLE_PROOF"]:
            self.handle_proof_request(payload)
        elif msg_type == self.network.MSG_TYPES["MERKLE_PROOF"]:
            self.handle_proof_response(payload)
        elif msg_type == self.network.MSG_TYPES["VIEW_CHANGE"]:
            self.handle_view_change(payload)
        elif msg_type == self.network.MSG_TYPES["TIMEOUT"]:
            self.handle_timeout(payload)
        else:
            logging.warning(f"[{self.id}] Unknown msg_type {msg_type}")
    
    # ==================== 提案处理（修复） ====================
    
    def handle_proposal(self, proposal):
        """处理提案消息（使用新的验证流程）"""
        logging.debug(f"[{self.id}] Received proposal at view {self.view}")
        
        # 反序列化
        block_dict = proposal.get("block")
        qc_dict = proposal.get("qc")
        view = proposal.get("view", 0)
        
        if not block_dict or not qc_dict:
            logging.warning(f"[{self.id}] Invalid proposal: missing block or qc")
            return
        
        from block import Block
        block = Block.from_dict(block_dict)
        qc = QC.from_dict(qc_dict)
        
        # 验证提案
        is_valid, reason = self.validate_proposal_new(block, qc, view)
        if not is_valid:
            logging.warning(f"[{self.id}] Proposal invalid: {reason}")
            # 如果怀疑，可以请求Merkle证明
            if self.should_request_proof(qc):
                self.verify_with_merkle_proof(qc, self.index)
                return False, "Merkle proof verification failed01"
            return
        
        # 存储当前提案
        self.current_proposal = {
            "block": block,
            "qc": qc,
            "view": view,
            "sender": proposal.get("sender")
        }
        
        # 投票
        self.create_and_send_vote(block, qc, view)
    
    def validate_proposal_new(self, block, qc: QC, view: int) -> Tuple[bool, str]:
        """新的提案验证（使用分层验证）"""
        if self.view == 0:
            # 创世QC不需要验证签名
            logger.info("Verifying genesis QC (special handling)")
            return True, "Genesis QC (special)"
        # 1. 基础检查
        if view < self.view:
            return False, f"view {view} < current view {self.view}"
        
        # 2. 检查block是否有效
        if not block.validate():
            return False, "Block validation failed"
        
        # 3. 检查proposer是否为当前leader
        leader_id = self.leader_for_view(view)
        if block.proposer != leader_id:
            return False, f"Proposer {block.proposer} != leader {leader_id}"
        
        # 4. 快速QC检查
        if qc.view >= view:
            return False, f"QC view {qc.view} >= proposal view {view}"
        
        # 5. 检查父哈希一致性
        if block.parent_hash != qc.block_hash:
            return False, f"Block parent_hash {block.parent_hash.hex()[:8]} != QC block_hash {qc.block_hash.hex()[:8]}"
        
        # 6. 分层验证QC
        qc_valid, qc_reason = self.verify_qc_as_replica(qc)
        if not qc_valid:
            return False, f"QC verification failed: {qc_reason}"
        
        return True, "OK"
    
    # ==================== QC验证（核心新增） ====================
    
    def verify_qc_as_replica(self, qc: QC) -> Tuple[bool, str]:
        """副本的分层QC验证"""
        # 创世QC的特殊处理
        if qc.view == 0:
            # 创世QC不需要验证签名
            logger.info("Verifying genesis QC (special handling)")
            return True, "Genesis QC (special)"
        # 1. 基本检查
        if qc.view < self.state.locked_qc.view:
            return False, f"QC view {qc.view} < locked view {self.state.locked_qc.view}"
        
        # 2. 快速位图检查
        if qc.get_signer_count() < 2 * self.f + 1:
            return False, f"Insufficient signers: {qc.get_signer_count()}"
        
        # 3. 检查自己是否在签名者中
        i_am_signer = qc.is_signer(self.index)
        
        # 4. 如果不在签名者中或怀疑，请求Merkle证明
        if not i_am_signer or self.state.suspicious_qc_count > 0:
            if not self.verify_with_merkle_proof(qc, self.index):
                return False, "Merkle proof verification failed"
        
        # 5. 最终聚合签名验证 view.to_bytes(8, 'big') + block.hash todo
        # if not BLS.verify_group_signature(self.group_pk, message, qc.aggregate_signature):
        message = qc.view.to_bytes(8, 'big') + qc.block_hash
        # if not self.bls.verify_group_signature(self.gpk, message, qc.aggregate_signature):
            # return False, "Aggregate signature verification failed"
            # return True, "Aggregate signature verification succ111"
        
        return True, "QC verified"
    
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
    
    # ==================== 投票创建（修复） ====================
    
    def create_and_send_vote(self, block, qc: QC, view: int):
        """创建并发送投票"""
        try:
            # 准备签名数据
            sign_data = view.to_bytes(8, 'big') + block.hash
            
            # 生成部分签名
            partial_sig = self.bls.sign_partial(self.priv, sign_data)
            
            # 保存自己的签名
            self.state.record_partial_sig(view, block.hash, partial_sig)
            
            # 创建投票对象
            vote = Vote(
                voter_id=self.id,
                voter_index=self.index,
                block_hash=block.hash,
                view=view,
                partial_signature=partial_sig,
                high_qc=self.state.latest_qc
            )
            
            # 发送投票（不再发送partial_signature）
            self.network.broadcast_vote(
                sender_id=self.id,
                block_id=block.id,
                view=view,
                partial_sig=partial_sig,
                voter_index=self.index
            )
            
            logging.debug(f"[{self.id}] Voted for block {block.id[:8]} at view {view}")
            
        except Exception as e:
            logging.error(f"[{self.id}] Failed to create vote: {e}")
    
    # ==================== 作为Leader的处理（新增） ====================
    
    def handle_vote(self, vote_dict):
        """处理收到的投票（Leader调用）"""
        if not self.is_leader:
            return
        
        try:
            logging.info(f"[{self.id}] Received vote dict: type={type(vote_dict)}, keys={list(vote_dict.keys()) if isinstance(vote_dict, dict) else 'not dict'}")
            vote = Vote.from_dict(vote_dict)

            # 确保有voter_id
            if 'voter_id' not in vote_dict:
                logger.error(f"[{self.id}] Vote missing voter_id: {vote_dict}")
                return
                        
            # 验证投票 todo
            # 需要获取投票者的spk_i
            # spk_i = self.get_spk_for_node(vote.voter_id)
            # valid, reason = vote.verify(spk_i, self.view, self.current_proposal["block"].hash)
            
            # if valid:
            #     self.collect_vote(vote)
            
            # 临时：假设验证通过
            self.collect_vote(vote)
            
        except Exception as e:
            logging.error(f"[{self.id}] Error processing vote: {e}")
    
    def collect_vote(self, vote: Vote):
        """收集投票（Leader）"""
        block_id = vote.block_hash.hex()
        
        if block_id not in self.pending_votes:
            self.pending_votes[block_id] = []
        
        # 去重
        if any(v.voter_id == vote.voter_id for v in self.pending_votes[block_id]):
            return
        
        self.pending_votes[block_id].append(vote)
        
        # 检查是否收集到足够投票
        if len(self.pending_votes[block_id]) >= 2 * self.f:
            self.assemble_qc(block_id)
    
    def assemble_qc(self, block_id_hex: str):
        """组装QC（Leader）"""
        votes = self.pending_votes.get(block_id_hex, [])
        if len(votes) < 2 * self.f + 1:
            logger.debug(f"[{self.id}] Not enough votes to assemble QC for block {block_id_hex[:8]}")
            return
        
        # 获取区块
        block = self.state.get_block(block_id_hex)
        if not block:
            logger.error(f"[{self.id}] Block {block_id_hex[:8]} not found for QC assembly")
            return
        
        # 构建signer_bitmap
        signer_bitmap = 0
        for vote in votes:
            signer_bitmap |= (1 << vote.voter_index)
            logger.debug(f"[{self.id}] Vote from {vote.voter_id} at index {vote.voter_index} included in QC assembly")
            if(vote.partial_signature is None):
                logger.error(f"[{self.id}] Vote from {vote.voter_id} has no partial_signature")
            if(vote.block_hash is None):
                logger.error(f"[{self.id}] Vote from {vote.voter_id} has no block_hash")
            if(vote.view is None):
                logger.error(f"[{self.id}] Vote from {vote.voter_id} has no view")
        
        # 构建Merkle树
        leaves_data = []
        partial_sigs = []
        for vote in votes:
            leaf_data = (
                vote.voter_index.to_bytes(4, 'big') +
                vote.partial_signature +
                vote.view.to_bytes(8, 'big') +
                vote.block_hash
            )
            leaves_data.append(leaf_data)
            partial_sigs.append(vote.partial_signature)
        
        # 构建Merkle树（简化）
        merkle_root = self.build_merkle_root(leaves_data)
        
        # 聚合签名（需要BLS库支持）
        aggregate_sig = self.bls.aggregate_partial_sigs(partial_sigs)
        # aggregate_sig = b"mock_aggregate_sig"  # 临时
        
        # 创建QC
        qc = QC(
            view=self.view,
            block_hash=block.hash,
            aggregate_signature=aggregate_sig,
            signer_bitmap=signer_bitmap,
            merkle_root=merkle_root
        )
        
        # 更新状态
        self.state.update_latest_qc(qc)
        
        # 广播NEW_VIEW消息
        self.network.broadcast_new_view(self.id, qc)
        
        # 清理
        self.pending_votes.pop(block_id_hex, None)
        
        logging.info(f"[{self.id}] Assembled QC for block {block_id_hex[:8]}")
    
    # ==================== Merkle证明处理（新增） ====================
    
    def handle_proof_request(self, payload):
        """处理Merkle证明请求（Leader调用）"""
        if not self.is_leader:
            return
        
        replica_index = payload.get("replica_index")
        view = payload.get("view")
        block_hash_hex = payload.get("block_hash")
        requester_id = payload.get("sender")
        
        if not all([replica_index, view, block_hash_hex, requester_id]):
            return
        
        block_hash = bytes.fromhex(block_hash_hex)
        
        # 查找该副本的partial signature
        # 这里需要Leader存储了所有partial signatures
        partial_sig = self.get_stored_partial_sig(replica_index, view, block_hash)
        if not partial_sig:
            return
        
        # 构建Merkle证明（需要存储Merkle树）
        proof = self.build_merkle_proof(replica_index, view, block_hash, partial_sig)
        if proof:
            self.network.send_merkle_proof(self.id, requester_id, proof)
    
    def handle_proof_response(self, payload):
        """处理Merkle证明响应"""
        proof_dict = payload.get("proof")
        sender = payload.get("sender")
        
        if not proof_dict:
            return
        
        proof = MerkleProof.from_dict(proof_dict)
        
        # 缓存证明供验证使用
        proof_key = (proof.view, proof.block_hash, proof.replica_index)
        if not hasattr(self, 'cached_proofs'):
            self.cached_proofs = {}
        self.cached_proofs[proof_key] = proof
        
        logging.debug(f"[{self.id}] Received Merkle proof from {sender}")
    
    # ==================== 辅助方法 ====================
    
    def leader_for_view(self, view: int) -> str:
        """计算指定视图的Leader"""
        if not self.all_nodes:
            return ""
        return self.all_nodes[view % len(self.all_nodes)]
    
    def should_request_proof(self, qc: QC) -> bool:
        """判断是否需要请求Merkle证明"""
        # 条件：自己不在签名者中，或QC视图跳跃过大，或之前有可疑记录
        if not qc.is_signer(self.index):
            return True
        if qc.view > self.state.latest_qc.view + 10:
            return True
        if self.state.suspicious_qc_count > 0:
            return True
        return False
    
    def build_merkle_root(self, leaves_data: List[bytes]) -> bytes:
        """构建Merkle树根（简化）"""
        if not leaves_data:
            return bytes([0] * 32)
        # 实际应使用Merkle树库
        all_data = b"".join(leaves_data)
        return hashlib.sha256(all_data).digest()
    
    def build_merkle_proof(self, index: int, view: int, block_hash: bytes, partial_sig: bytes) -> Optional[MerkleProof]:
        """构建Merkle证明（Leader）"""
        # 需要Leader存储了完整的Merkle树
        # 这里返回简化版本
        return MerkleProof(
            replica_index=index,
            partial_signature=partial_sig,
            view=view,
            block_hash=block_hash,
            sibling_hashes=[]  # 实际需要从Merkle树中获取
        )
    
    def get_stored_partial_sig(self, index: int, view: int, block_hash: bytes) -> Optional[bytes]:
        """获取存储的部分签名（Leader）"""
        # 需要Leader存储所有投票的partial signatures
        # 这里简化返回
        return None
    
    # ==================== 旧方法的兼容性处理 ====================
    # 以下方法可以暂时保留，但应该逐渐迁移到新流程
    
    def on_receive_proposal(self, proposal):
        """旧提案处理方法（兼容）"""
        logging.warning(f"[{self.id}] Using deprecated on_receive_proposal")
        self.handle_proposal(proposal)
    
    def vote(self, block, view):
        """旧投票方法（兼容）"""
        logging.warning(f"[{self.id}] Using deprecated vote method")
        self.create_and_send_vote(block, None, view)
    
    def compute_root_from_proof(self, leaf_hash: bytes, sibling_hashes: List[bytes]) -> bytes:
    # """
    # 从叶子哈希和兄弟哈希列表计算Merkle根（核心验证逻辑）
    # :param leaf_hash: 目标叶子节点的哈希（bytes）
    # :param sibling_hashes: Merkle证明的兄弟哈希列表（按从下到上顺序，每个元素为bytes）
    # :return: 计算出的Merkle根（bytes）
    # """
        if not leaf_hash:
            logging.error(f"[{self.id}] Empty leaf hash for Merkle proof")
            return bytes([0] * 32)  # 返回空根（32字节0）
        
        current_hash = leaf_hash
        
        # 逐层向上计算：每一层将当前哈希与兄弟哈希拼接后再哈希
        for idx, sibling_hash in enumerate(sibling_hashes):
            if not sibling_hash:
                logging.warning(f"[{self.id}] Empty sibling hash at level {idx}")
                sibling_hash = bytes([0] * 32)
            
            # 关键：Merkle树哈希拼接规则（左小右大，保证顺序一致）
            # 比较当前哈希和兄弟哈希的字节序，确保拼接顺序固定（避免左右顺序错误）
            if current_hash < sibling_hash:
                combined = current_hash + sibling_hash
            else:
                combined = sibling_hash + current_hash
            
            # 使用SHA256计算下一层哈希（与Merkle树根生成算法保持一致）
            current_hash = hashlib.sha256(combined).digest()
        
        return current_hash
    
    def sign_transaction(self, message: bytes) -> bytes:
        """DID签名：用于交易签名"""
        return BLS.sign(self.did_priv, message)
    
    def verify_did_signature(self, message: bytes, signature: bytes, pub_key) -> bool:
        """验证DID签名"""
        return BLS.verify(pub_key, message, signature)

    def sign_consensus(self, message: bytes) -> bytes:
        """使用BLS私钥为共识消息签名"""
        from crypto import BLS
        return BLS.sign(self.priv_key, message)


####todo
        # ==================== 新视图处理 ====================
    
    def handle_new_view(self, payload):
        """处理NEW-VIEW消息（包含新QC）"""
        try:
            sender = payload.get("sender")
            qc_dict = payload.get("qc")
            view = payload.get("view", 0)

            if view == 0:
                # 创世QC不需要验证签名
                logger.info("handle genesis QC (special handling)")
            
            logger.info(f"[{self.id}] Received NEW-VIEW from {sender}, view={view}")
            
            if not qc_dict:
                logger.warning(f"[{self.id}] Invalid NEW-VIEW: missing QC")
                return
            
            # 反序列化QC
            qc = QC.from_dict(qc_dict)
            
            # 验证QC的基本有效性
            if not self._validate_new_view_qc(qc, sender, view):
                logger.warning(f"[{self.id}] NEW-VIEW QC validation failed")
                return
            
            # 更新本地状态
            self._update_state_with_new_qc(qc)
            
            # 检查是否需要成为下一个Leader
            self._check_and_become_next_leader(qc.view)
            
            logger.info(f"[{self.id}] Successfully processed NEW-VIEW for view {qc.view}")
            
        except Exception as e:
            logger.error(f"[{self.id}] Error handling NEW-VIEW: {e}", exc_info=True)
    
    def _validate_new_view_qc(self, qc: QC, sender: str, view: int) -> bool:
        """验证NEW-VIEW消息中的QC"""
        # 1. 验证发送者是否为前一个视图的Leader todo
        expected_leader = self.leader_for_view(view - 1)
        if sender != expected_leader:
            logger.warning(f"[{self.id}] NEW-VIEW sender {sender} not the leader of previous view {view-1}")
            # 这可能不是致命错误，但值得注意
            # return False  # 根据协议严格性决定
        
        # 2. 验证QC的视图号
        # if qc.view != view - 1:  # NEW-VIEW消息应该携带前一个视图的QC
        #     logger.warning(f"[{self.id}] QC view {qc.view} doesn't match expected view {view-1}")
        #     return False
        
        # 3. 验证QC本身（使用分层验证）
        qc_valid, reason = self.verify_qc_as_replica(qc)
        if not qc_valid:
            logger.warning(f"[{self.id}] NEW-VIEW QC verification failed: {reason}")
            return False
        
        # 4. 检查QC是否比当前最新QC更新
        if qc.view <= self.state.latest_qc.view:
            logger.debug(f"[{self.id}] NEW-VIEW QC view {qc.view} <= current latest view {self.state.latest_qc.view}")
            # 这不是错误，但可能不需要更新
            # return False
        
        return True
    
    def _update_state_with_new_qc(self, qc: QC):
        """用新的QC更新本地状态"""
        # 1. 更新latest_qc
        if self.state.update_latest_qc(qc):
            logger.info(f"[{self.id}] Updated latest_qc to view {qc.view}")
        
        # 2. 尝试更新locked_qc（如果满足条件）
        if self._should_update_locked_qc(qc):
            if self.state.update_locked_qc(qc):
                logger.info(f"[{self.id}] Updated locked_qc to view {qc.view}")
        
        # 3. 尝试提交区块
        self._try_commit_with_new_qc(qc)
    
    def _should_update_locked_qc(self, qc: QC) -> bool:
        """判断是否应该更新locked_qc"""
        # HotStuff的锁定规则：
        # 如果新QC的视图比当前locked_qc高，并且新QC的父QC视图 >= 当前locked_qc的视图
        # 则更新locked_qc
        
        # 获取新QC对应的区块
        if(qc.block_hash is None):
            logger.warning(f"[{self.id}] Cannot update locked_qc: qc.block_hash is None for QC {qc.view}")
            return False
        block = self.state.get_block_by_hash(qc.block_hash)
        # if not block or not block.qc:
        if block is None:
            logger.warning(f"[{self.id}] Cannot update locked_qc: block not found for QC {qc.view}")
            return False
        
        # 检查新QC的父QC
        parent_qc = block.qc
        
        # 规则：新QC的父QC视图 >= 当前locked_qc的视图
        if parent_qc is None:
            logger.warning(f"[{self.id}] Cannot update locked_qc: parent_qc is None for block {block.id[:8]}")
            # return False
        if self.state.locked_qc is None:
            logger.warning(f"[{self.id}] Cannot update locked_qc: locked_qc is Nonehhhhh")
            return False

        # if parent_qc.view >= self.state.locked_qc.view:
        #     return True 
        #todo

        return True
        
        return False
    
    def _try_commit_with_new_qc(self, qc: QC):
        """尝试使用新的QC提交区块"""
        # 获取QC对应的区块
        if(qc.block_hash is None):
            logger.warning(f"[{self.id}] 222Cannot update locked_qc: qc.block_hash is None for QC {qc.view}")
            return False
        block = self.state.get_block_by_hash(qc.block_hash)
        # if not block or not block.qc:
        if block is None:
            logger.warning(f"[{self.id}] Cannot commit: block not found for QC {qc.view}")
            return
        
        
        # HotStuff的三链提交规则：
        # 需要三个连续的区块 B0, B1, B2 满足：
        # 1. B2有QC (当前qc)
        # 2. B1有QC (B2.parent_qc)
        # 3. B0有QC (B1.parent_qc)
        
        try:
            # 检查三链
            b2 = block  # 当前区块
            # b1 = self.state.get_block_by_hash(b2.parent_hash)
            
            # if not b1 or not b1.qc:
            #     return
            
            # b0 = self.state.get_block_by_hash(b1.parent_hash)
            # if not b0 or not b0.qc:
            #     return
            
            # 检查连续性和QC关系
            # if (b2.height == b1.height + 1 == b0.height + 2 and
            #     b2.qc.view >= b1.qc.view >= b0.qc.view):
                
            #     # 提交b0
            #     self._commit_block(b0)
            #     logger.info(f"[{self.id}] Committed block {b0.id[:8]} via three-chain rule")

            self._commit_block(b2)
                
        except Exception as e:
            logger.error(f"[{self.id}] Error in three-chain commit check: {e}")
    
    def _commit_block(self, block):
        """提交区块到世界状态"""
        try:
            logger.info(f"[{self.id}] COMMITTING block {block.id[:8]}, height={block.height}, view={block.view}")
            
            # 调试 block.payload
            logger.debug(f"[{self.id}] block.payload type: {type(block.payload)}")
            # logger.debug(f"[{self.id}] block.payload: {block.payload}")

            # 应用区块中的交易到世界状态 todo
            for tx in block.payload:
                logger.debug(f"[{self.id}] 12.tx type: {type(tx)}")
                # self.world_state.apply_transaction(tx)
            
            # 更新已提交的QC
            if block.qc:
                logger.debug(f"[{self.id}] 23.block.qc type: {type(block.qc)}")
                self.state.update_commit_qc(block.qc)
            
            # 记录指标
            if self.metrics:
                logger.debug(f"[{self.id}] 34.metrics type: {type(self.metrics)}")
                # self.metrics.record_block_commit(block.height, block.view)
            
        except Exception as e:
            logger.error(f"[{self.id}] Error committing block {block.id[:8]}: {e}")
    
    def _check_and_become_next_leader(self, qc_view: int):
        """检查是否需要成为下一个Leader"""
        next_view = qc_view + 1
        next_leader = self.leader_for_view(next_view)
        
        if next_leader == self.id:
            logger.info(f"[{self.id}] I am the leader for next view {next_view}")
            self._prepare_for_leadership(next_view)
        else:
            logger.debug(f"[{self.id}] Next leader is {next_leader}, not me")
    
    def _prepare_for_leadership(self, view: int):
        """为成为Leader做准备"""
        # 设置视图号
        self.view = view
        self.is_leader = True
        
        logger.info(f"[{self.id}] Preparing to lead view {view}")
        
        # 这里可以启动Leader的提案流程
        # 在实际实现中，可能需要启动一个定时器或直接开始提案
        # 为了简化，我们可以在共识模块中处理
        
        # 如果当前有内存池，可以开始创建提案
        if hasattr(self, 'mempool'):
            # 创建新区块并广播提案
            # 这个逻辑可能在共识模块中统一处理
            pass