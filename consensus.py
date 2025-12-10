# consensus.py (完整重写版本)
import threading
import time
import hashlib
from typing import Dict, List, Optional, Tuple
from block import Block
from qc import QC, MerkleProof
from votes import Vote
from mempool import Mempool
from network import Network
from state import NodeState
from utils.logger import event

# 配置常量
TXS_PER_PROPOSAL = 2
VOTE_TIMEOUT = 2.0  # 收集投票的超时时间（秒）
PROOF_REQUEST_TIMEOUT = 0.5  # 请求Merkle证明的超时时间
MAX_ROUNDS_WITHOUT_PROGRESS = 3  # 连续无进展的轮数阈值

class Consensus:
    def __init__(self, nodes: Dict[str, any], network: Network, f: int = 1,max_rounds=10):
        self.nodes = nodes
        self.network = network
        self.f = f
        self.n = len(nodes)
        self.group_pk = None 
        self.max_rounds = max_rounds
        
        # 共识状态
        self.view = 1
        self.active = True
        self.current_leader_id = None
        self.current_round = 0
        
        # 当前视图的投票收集
        self.votes_received = {}  # voter_id -> Vote
        self.vote_collection_thread = None
        
        # Merkle证明请求跟踪
        self.pending_proof_requests = {}  # request_id -> (timestamp, callback)
        
        # 性能监控
        self.stats = {
            "proposals_made": 0,
            "qcs_formed": 0,
            "view_changes": 0,
            "avg_vote_collection_time": 0.0,
        }
        
        # 为每个节点初始化状态（如果尚未初始化）
        for node_id, node in self.nodes.items():
            if not hasattr(node, 'state') or node.state is None:
                node.state = NodeState(node_id=node_id, f=f)
        
        # 启动共识线程
        self.consensus_thread = threading.Thread(target=self._run_consensus, daemon=True)
        self.message_handler_thread = threading.Thread(target=self._handle_messages, daemon=True)
        
    def start(self):
        """启动共识协议"""
        print(f"[Consensus] Starting consensus with view={self.view}, nodes={list(self.nodes.keys())}")
        
        self.active = True
        self.consensus_thread.start()
        self.message_handler_thread.start()
        
        print(f"[Consensus] Threads started: consensus_thread={self.consensus_thread.is_alive()}, "
            f"message_handler_thread={self.message_handler_thread.is_alive()}")
        
        return self.consensus_thread
    
    def stop(self):
        """停止共识协议"""
        self.active = False
        # event("consensus_stopped", node_id="system", view=self.view, consensus_type="my")
    
    # def _run_consensus(self):
    #     """主共识循环：节点根据当前视图决定是Leader还是Replica"""
    #     print(f"[Consensus] Starting consensus loop, active={self.active}")
        
    #     loop_count = 0
    #     while self.active:
    #         loop_count += 1
            
    #         # 确定当前视图的Leader
    #         leader_id = self.leader_for_view(self.view)
    #         self.current_leader_id = leader_id
            
    #         print(f"[Consensus] Loop {loop_count}: view={self.view}, leader={leader_id}")
            
    #         # 获取当前节点
    #         my_id = self._get_my_node_id()
    #         myself = self.nodes.get(my_id)
            
    #         if not myself:
    #             print(f"[Consensus] WARNING: Could not find myself {my_id} in nodes")
    #             time.sleep(0.1)
    #             continue
            
    #         # 标记是否为Leader
    #         is_leader = (my_id == leader_id)
    #         myself.is_leader = is_leader
            
    #         print(f"[Consensus] Node {my_id} is_leader={is_leader}")
            
    #         if is_leader:
    #             print(f"[Consensus] Node {my_id} entering leader phase")
    #             self._leader_phase(myself)
    #         else:
    #             print(f"[Consensus] Node {my_id} entering replica phase, waiting for proposal from {leader_id}")
    #             self._replica_phase(myself, leader_id)
            
    #         # 短暂暂停后进入下一视图
    #         self.view += 1
    #         print(f"[Consensus] Advancing to next view: {self.view}")
    #         time.sleep(0.5)  # 增加暂停时间以便观察
        
    #     print("[Consensus] Consensus loop stopped")

    # consensus.py - 修改 _run_consensus 方法
    def _run_consensus(self):
        """主共识循环：全局协调所有节点"""
        print(f"[Consensus] Starting global consensus loop")

        consecutive_failures = 0  # 跟踪连续失败次数
        max_consecutive_failures = 3  # 最多允许连续失败3次
        
        while self.active and self.current_round < self.max_rounds:
            current_view = self.view
            leader_id = self.leader_for_view(current_view)
            
            print(f"[Consensus] Round {self.current_round}: view={current_view}, leader={leader_id}")

            # 检查leader是否存在
            if leader_id not in self.nodes:
                print(f"[Consensus] ERROR: Leader {leader_id} not found in nodes!")
                break
            
            # 设置所有节点的leader状态
            for node_id, node in self.nodes.items():
                node.view = current_view
                node.is_leader = (node_id == leader_id)
                print(f"[Consensus] Setting node {node_id}.is_leader={node.is_leader}")
            
            # Leader创建并广播提案
            leader_node = self.nodes[leader_id]
            print(f"[Consensus] Leader {leader_id} creating proposal...")
            
            # 1. Leader创建区块
            block = self._create_new_block(leader_node)
            if not block:
                print(f"[Consensus] Failed to create block for leader {leader_id}")
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    print(f"[Consensus] Too many consecutive failures ({consecutive_failures}), stopping")
                self.view += 1
                time.sleep(0.1)
                continue
            
            print(f"[Consensus] Leader {leader_id} created block {block.id[:8]}")
            consecutive_failures = 0
            
            # 2. Leader广播提案到所有副本
            self._broadcast_proposal(leader_node, block)
            
            # 3. 模拟所有副本接收并投票
            votes = []
            for node_id, node in self.nodes.items():
                if node_id == leader_id:
                    continue  # Leader不给自己投票
                    
                print(f"[Consensus] Node {node_id} processing proposal...")
                
                # 验证提案
                is_valid, reason = self._validate_proposal(node, block, leader_node.state.latest_qc)
                if is_valid:
                    # 创建投票
                    vote = self._create_vote(node, block, current_view)
                    if vote:
                        votes.append(vote)
                        print(f"[Consensus] Node {node_id} voted for block {block.id[:8]}")
            
            # 4. Leader收集投票并形成QC
            if len(votes) >= 2 * self.f:
                print(f"[Consensus] Leader {leader_id} collected {len(votes)} votes, forming QC...")
                qc = self._assemble_qc_from_votes(block.hash, {v.voter_id: v for v in votes})
                
                if qc:
                    # 更新所有节点的状态
                    for node in self.nodes.values():
                        node.state.update_latest_qc(qc)
                    
                    # 广播NEW-VIEW
                    self._broadcast_new_view(leader_node, qc)
                    
                    print(f"[Consensus] QC formed for view {current_view}, block {block.id[:8]}")
                    self.stats["qcs_formed"] += 1
            
            self.stats["proposals_made"] += 1
            self.current_round += 1
            self.view += 1
            
            print(f"[Consensus] Round {self.current_round} completed, advancing to view {self.view}")
            time.sleep(0.5)  # 模拟一轮的时间
        
        print("[Consensus] Consensus loop completed")
    
    def _leader_phase(self, leader_node):
        """Leader阶段：创建提案并收集投票"""
        leader_id = leader_node.id
        self.current_round += 1
        
        # 1. 创建新区块
        block = self._create_new_block(leader_node)
        if not block:
            # event("block_creation_failed", node_id=leader_id, view=self.view, consensus_type="my")
            return
        
        # 2. 广播提案
        self._broadcast_proposal(leader_node, block)
        self.stats["proposals_made"] += 1
        
        # 3. 等待并收集投票
        qc = self._collect_votes_and_form_qc(leader_node, block)
        
        if qc:
            # 4. 成功形成QC，广播NEW-VIEW消息
            self._broadcast_new_view(leader_node, qc)
            
            # 5. 更新本地状态
            leader_node.state.update_latest_qc(qc)
            if qc.view > leader_node.state.locked_qc.view:
                leader_node.state.update_locked_qc(qc)
            
            self.stats["qcs_formed"] += 1
            # event("qc_formed_success", node_id=leader_id, view=self.view, 
            #       block_id=block.id, qc_view=qc.view, consensus_type="my")
        else:
            # 投票收集失败，等待超时后进入下一视图
            # event("vote_collection_failed", node_id=leader_id, view=self.view, 
            #       block_id=block.id, consensus_type="my")
            time.sleep(VOTE_TIMEOUT)
        
        # 6. 进入下一视图
        self.view += 1
    
    def _replica_phase(self, replica_node, leader_id):
        """Replica阶段：等待提案，验证并投票"""
        replica_id = replica_node.id
        
        # 1. 等待提案（带超时）
        proposal = self._wait_for_proposal(replica_node, leader_id)
        if not proposal:
            # 超时，可能触发视图切换
            # event("proposal_timeout", node_id=replica_id, view=self.view, 
            #       leader_id=leader_id, consensus_type="my")
            return
        
        block = proposal.get("block")
        proposal_qc = proposal.get("qc")
        
        # 2. 验证提案
        is_valid, reason = self._validate_proposal(replica_node, block, proposal_qc)
        if not is_valid:
            # event("proposal_invalid", node_id=replica_id, view=self.view, 
            #       block_id=block.id, reason=reason, consensus_type="my")
            return
        
        # 3. 验证提案中的QC（使用分层验证）
        if proposal_qc:
            qc_valid, qc_reason = self._verify_qc_as_replica(replica_node, proposal_qc)
            if not qc_valid:
                # event("qc_verification_failed", node_id=replica_id, view=self.view, 
                #       reason=qc_reason, consensus_type="my")
                # 可以请求Merkle证明进行深度验证
                self._request_merkle_proof_if_needed(replica_node, proposal_qc)
                return
        
        # 4. 对提案投票
        vote = self._create_vote(replica_node, block)
        if vote:
            # 广播投票
            self.network.broadcast_vote(
                sender_id=replica_id,
                block_id=block.id,
                view=self.view,
                partial_sig=vote.partial_signature,
                voter_index=replica_node.index  # 假设节点有index属性
            )
            
            # 记录投票
            replica_node.state.record_vote(block.id, self.view, vote.partial_signature)
            # event("vote_cast", node_id=replica_id, view=self.view, block_id=block.id, consensus_type="my")
    
    def _create_new_block(self, leader_node) -> Optional[Block]:
        """创建新区块"""
        try:
            # 获取父区块哈希
            print(f"[Consensus] Leader {leader_node.id} creating new block at height {leader_node.state.height + 1}")
            parent_hash = leader_node.state.latest_qc.block_hash

            print(f"[Consensus] Parent hash: {parent_hash.hex() if parent_hash else 'None'}")
            
            # 从内存池获取交易
            # txs = Mempool.get_txs(TXS_PER_PROPOSAL)
            txs = self.mempool.get_txs(TXS_PER_PROPOSAL)
            
            print(f"[Consensus] Transactions for new block: {txs}")

            # 创建新区块
            block = Block(
                parent_hash=parent_hash,
                height=leader_node.state.height + 1,
                proposer=leader_node.id,
                payload=txs,
                qc=leader_node.state.latest_qc,  # 指向父区块的QC
                view=self.view
            )
            
            print(f"[Consensus] New block created: id={block.id[:8]}, height={block.height}, view={block.view}")

            # 添加到本地状态
            leader_node.state.add_block(block)
            
            # event("block_created", node_id=leader_node.id, view=self.view, 
            #       block_id=block.id, height=block.height, tx_count=len(txs), consensus_type="my")
            
            return block
            
        except Exception as e:
            # event("block_creation_error", node_id=leader_node.id, view=self.view, 
            #       error=str(e), consensus_type="my")
            error_msg = f"[Consensus] Block creation failed (leader={leader_node.id}, view={self.view}): {str(e)}"
            print(error_msg)
            return None
    
    def _broadcast_proposal(self, leader_node, block):
        """广播提案"""
        self.network.broadcast_proposal(
            sender_id=leader_node.id,
            block=block,
            qc=leader_node.state.latest_qc,
            view=self.view
        )
        
        # event("proposal_broadcast", node_id=leader_node.id, view=self.view, 
        #       block_id=block.id, height=block.height, consensus_type="my")
    
    def _collect_votes_and_form_qc(self, leader_node, block) -> Optional[QC]:
        """收集投票并形成QC"""
        leader_id = leader_node.id
        votes_collected = {}
        start_time = time.time()
        
        # 设置超时
        while time.time() - start_time < VOTE_TIMEOUT:
            # 检查已收集的投票
            for voter_id, vote in self.votes_received.items():
                if voter_id not in votes_collected:
                    # 验证投票
                    is_valid, reason = vote.verify(
                        spk_i=self._get_spk_for_node(voter_id),  # 需要实现
                        expected_view=self.view,
                        expected_block_hash=block.hash
                    )
                    
                    if is_valid:
                        votes_collected[voter_id] = vote
                        # event("vote_received", node_id=leader_id, view=self.view, 
                        #       voter_id=voter_id, consensus_type="my")
            
            # 检查是否收集到足够投票
            if len(votes_collected) >= 2 * self.f:
                break
            
            time.sleep(0.01)  # 短暂等待
        
        # 计算收集时间
        collection_time = time.time() - start_time
        self.stats["avg_vote_collection_time"] = (
            self.stats["avg_vote_collection_time"] * (self.stats["qcs_formed"] - 1) + collection_time
        ) / max(1, self.stats["qcs_formed"])
        
        # 检查是否达到法定票数
        if len(votes_collected) < 2 * self.f:
            # event("insufficient_votes", node_id=leader_id, view=self.view, 
            #       collected=len(votes_collected), required=2*self.f, consensus_type="my")
            return None
        
        # 组装QC
        qc = self._assemble_qc_from_votes(block.hash, votes_collected)
        return qc
    
    def _assemble_qc_from_votes(self, block_hash: bytes, votes: Dict[str, Vote]) -> Optional[QC]:
        """从投票集合组装QC"""
        if len(votes) < 2 * self.f + 1:
            return None
        
        # 准备数据
        leaves_data = []
        signer_bitmap = 0
        partial_sigs = []
        
        for vote in votes.values():
            # 设置位图
            signer_bitmap |= (1 << vote.voter_index)
            
            # 构造叶节点数据
            leaf_data = (
                vote.voter_index.to_bytes(4, 'big') +
                vote.partial_signature +
                vote.view.to_bytes(8, 'big') +
                block_hash
            )
            leaves_data.append(leaf_data)
            partial_sigs.append(vote.partial_signature)
        
        # 构建Merkle树（简化版，实际需用Merkle树库）
        merkle_root = self._build_merkle_root(leaves_data)
        
        # 聚合签名（简化，实际需用BLS聚合）
        aggregate_sig = self._aggregate_signatures(partial_sigs)
        
        # 创建QC
        qc = QC(
            view=self.view,
            block_hash=block_hash,
            aggregate_signature=aggregate_sig,
            signer_bitmap=signer_bitmap,
            merkle_root=merkle_root
        )
        
        return qc
    
    def _broadcast_new_view(self, leader_node, qc: QC):
        """广播NEW-VIEW消息（携带新QC）"""
        self.network.broadcast_new_view(
            sender_id=leader_node.id,
            qc=qc
        )
        
        # event("new_view_broadcast", node_id=leader_node.id, view=self.view, 
        #       qc_view=qc.view, consensus_type="my")
    
    def _wait_for_proposal(self, replica_node, leader_id, timeout=VOTE_TIMEOUT) -> Optional[dict]:
        """等待提案（带超时）"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            # 检查是否收到提案（实际实现中，这会通过消息队列）
            # 这里简化处理
            if hasattr(replica_node, 'last_proposal') and replica_node.last_proposal:
                proposal = replica_node.last_proposal
                # 验证来自正确的Leader
                if proposal.get("sender") == leader_id and proposal.get("view") == self.view:
                    replica_node.last_proposal = None  # 清空已处理的提案
                    return proposal
            
            time.sleep(0.01)
        
        return None
    
    def _validate_proposal(self, replica_node, block, proposal_qc) -> Tuple[bool, str]:
        """验证提案的基本有效性"""
        # 1. 验证区块基本结构
        if not block.validate():
            return False, "Block validation failed"
        
        # 2. 验证提案者是否为当前Leader
        leader_id = self.leader_for_view(self.view)
        if block.proposer != leader_id:
            return False, f"Proposer {block.proposer} is not leader {leader_id}"
        
        # 3. 验证视图号
        if block.view != self.view:
            return False, f"Block view {block.view} doesn't match current view {self.view}"
        
        # 4. 验证QC视图（如果提供了QC）
        if proposal_qc and proposal_qc.view >= self.view:
            return False, f"QC view {proposal_qc.view} >= current view {self.view}"
        
        # 5. 验证区块高度连续性
        if block.height != replica_node.state.height + 1:
            return False, f"Block height {block.height} not continuous with current height {replica_node.state.height}"
        
        return True, "OK"
    
    def _verify_qc_as_replica(self, replica_node, qc: QC) -> Tuple[bool, str]:
        """副本验证QC（分层验证）"""
        my_index = replica_node.index  # 假设节点知道自己的索引
        
        # 1. 基本检查
        if qc.view < replica_node.state.locked_qc.view:
            return False, f"QC view {qc.view} < locked view {replica_node.state.locked_qc.view}"
        
        # 2. 快速位图检查
        if qc.get_signer_count() < 2 * self.f + 1:
            return False, f"Insufficient signers: {qc.get_signer_count()} < {2 * self.f + 1}"
        
        # 3. 检查自己是否在签名者中（快速路径）
        i_am_signer = qc.is_signer(my_index)
        
        # 4. 如果自己不在签名者中或怀疑QC，请求Merkle证明
        if not i_am_signer or replica_node.state.suspicious_qc_count > 0:
            proof = self._request_and_verify_merkle_proof(replica_node, qc, my_index)
            if not proof:
                return False, "Merkle proof verification failed"
        
        # 5. 最终聚合签名验证（最昂贵，最后进行）
        # 需要实现subset公钥聚合和BLS验证
        # subset_pk = self._aggregate_public_keys_for_bitmap(qc.signer_bitmap)
        # message = qc.view.to_bytes(8, 'big') + qc.block_hash
        # if not bls.verify(subset_pk, qc.aggregate_signature, message):
        #     return False, "Aggregate signature verification failed"
        
        return True, "QC verified"
    
    def _request_and_verify_merkle_proof(self, replica_node, qc: QC, replica_index: int) -> bool:
        """请求并验证Merkle证明"""
        leader_id = self.leader_for_view(qc.view)
        replica_id = replica_node.id
        
        # 发送证明请求
        self.network.request_merkle_proof(
            requester_id=replica_id,
            leader_id=leader_id,
            replica_index=replica_index,
            view=qc.view,
            block_hash=qc.block_hash
        )
        
        # 等待响应（带超时）
        start_time = time.time()
        while time.time() - start_time < PROOF_REQUEST_TIMEOUT:
            # 检查是否收到证明（实际通过消息队列）
            if hasattr(replica_node, 'last_merkle_proof') and replica_node.last_merkle_proof:
                proof_dict = replica_node.last_merkle_proof
                replica_node.last_merkle_proof = None
                
                # 反序列化证明
                proof = MerkleProof.from_dict(proof_dict)
                
                # 验证证明
                # 1. 验证partial signature
                # 2. 验证Merkle路径
                # 这里简化处理
                if proof.replica_index == replica_index and proof.view == qc.view:
                    # 验证Merkle路径（需要实现）
                    # if self._verify_merkle_path(proof, qc.merkle_root):
                    #     return True
                    return True  # 简化：假设验证通过
            
            time.sleep(0.01)
        
        return False
    
    def _create_vote(self, replica_node, block) -> Optional[Vote]:
        """创建投票"""
        try:
            # 获取签名私钥
            signer = replica_node.signer  # 假设节点有signer属性
            
            # 准备签名数据
            sign_data = self.view.to_bytes(8, 'big') + block.hash
            
            # 生成部分签名
            partial_sig = signer.sign(sign_data)
            
            # 保存自己的签名
            replica_node.state.record_partial_sig(self.view, block.hash, partial_sig)
            
            # 创建投票对象
            vote = Vote(
                voter_id=replica_node.id,
                voter_index=replica_node.index,  # 假设节点有index属性
                block_hash=block.hash,
                view=self.view,
                partial_signature=partial_sig,
                high_qc=replica_node.state.latest_qc
            )
            
            return vote
            
        except Exception as e:
            # event("vote_creation_error", node_id=replica_node.id, view=self.view, 
            #       error=str(e), consensus_type="my")
            return None
    
    def _should_change_view(self) -> bool:
        """检查是否需要视图切换"""
        # 简化逻辑：如果连续多轮没有进展，可能需要视图切换
        # 实际实现中，这需要更复杂的条件
        return False
    
    def _initiate_view_change(self, node):
        """发起视图切换"""
        new_view = self.view + 1
        self.stats["view_changes"] += 1
        
        self.network.broadcast_view_change(
            sender_id=node.id,
            new_view=new_view,
            reason="no_progress",
            high_qc=node.state.latest_qc
        )
        
        # event("view_change_initiated", node_id=node.id, view=self.view, 
        #       new_view=new_view, consensus_type="my")
        
        # 更新视图
        self.view = new_view
    
    def _handle_messages(self):
        """处理网络消息（后台线程）"""
        while self.active:
            # 在实际实现中，这会从网络消息队列中获取并处理消息
            # 这里简化处理
            time.sleep(0.01)
    
    def _get_my_node_id(self) -> str:
        """获取当前节点的ID（简化：返回第一个节点ID）"""
        # 在实际实现中，每个共识实例应该关联一个特定节点
        node_ids = list(self.nodes.keys())
        return node_ids[0] if node_ids else ""
    
    def _get_spk_for_node(self, node_id: str) -> bytes:
        """获取指定节点的签名公钥份额（需要实现）"""
        # 在实际实现中，这应该从DKG结果或配置中获取
        return b"mock_spk"
    
    def _build_merkle_root(self, leaves_data: List[bytes]) -> bytes:
        """构建Merkle树根（简化实现）"""
        # 实际应使用Merkle树库
        if not leaves_data:
            return bytes([0] * 32)
        
        # 简化：将所有叶子哈希连接后再次哈希
        all_data = b"".join(leaves_data)
        return hashlib.sha256(all_data).digest()
    
    def _aggregate_signatures(self, signatures: List[bytes]) -> bytes:
        """聚合签名（简化实现）"""
        # 实际应使用BLS聚合
        if not signatures:
            return b""
        
        # 简化：连接所有签名后取哈希
        all_sigs = b"".join(signatures)
        return hashlib.sha256(all_sigs).digest()[:48]  # 模拟BLS签名长度
    
    def leader_for_view(self, view: int) -> str:
        """计算指定视图的Leader"""
        ids = sorted(self.nodes.keys())
        if not ids:
            return ""
        return ids[view % len(ids)]
    
    def get_stats(self) -> dict:
        """获取共识统计信息"""
        return self.stats.copy()