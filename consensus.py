import logging
import time
from typing import Dict, List, Optional, Any
from block import Block
# from qc import QC
from network import Network
from votes import Vote
from ProposalValidator import ProposalValidator
from qc_verifier import QCVerifier
from vote_manager import VoteManager
from merkle_utils import MerkleUtils
import threading
from utils.logger import event
# from node import Node
from services import services
from typing import TYPE_CHECKING
from qc import QC, MerkleProof
# from import pacemaker


if TYPE_CHECKING:
    from node import Node



logger = logging.getLogger(__name__)

# 配置常量
TXS_PER_PROPOSAL = 20
VOTE_TIMEOUT = 2.0  # 收集投票的超时时间（秒）
PROOF_REQUEST_TIMEOUT = 0.5  # 请求Merkle证明的超时时间
MAX_ROUNDS_WITHOUT_PROGRESS = 3  # 连续无进展的轮数阈值


class Consensus:

    def __init__(self, nodes: Dict[str, any], network: Network, f: int = 1, max_rounds=10):
        self.nodes = nodes
        self.network = network
        self.f = f
        self.n = len(nodes)
        self.max_rounds = max_rounds
        self.services = None  # 占位符，注册时自动注入
        self.mempool = None

        
        # 从第一个节点获取组公钥（所有节点应该有相同的group_pk）
        first_node = list(self.nodes.values())[0] if self.nodes else None
        self.gpk = getattr(first_node, 'gpk', None)
        
        # 构建公钥映射
        self.public_key_map = {}
        for node_id, node in self.nodes.items():
            # 如果节点有公钥属性，添加到映射中
            if hasattr(node, 'pub'):
                self.public_key_map[node_id] = node.pub
            else:
                logger.warning(f"Node {node_id} does not have pub attribute")
        
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

        # 视图切换相关状态
        self.view_change_requests = {}  # view -> {node_id: ViewChangeRequest}
        self.view_change_timeouts = {}  # node_id -> timer
        self.max_view_change_timeout = 5.0  # 视图切换超时时间
        
        # 超时相关状态
        self.waiting_for_votes = False  # 是否正在等待投票
        self.vote_timeout_timer = None  # 投票超时定时器
        self.proposal_timeout_timer = None  # 提案超时定时器
        
        # 视图切换配置
        self.max_consecutive_timeouts = 3  # 最大连续超时次数
        self.consecutive_timeouts = 0  # 连续超时次数
        
        # 故障检测
        self.suspected_nodes = set()  # 被怀疑的节点
        self.node_failure_counts = {}  # 节点失败计数
        
        logger.info(f"[Consensus] 初始化超时和视图切换机制，故障容错f={f}")
        
        # === 创建创世区块和创世QC ===
        self.genesis_block = self._create_genesis_block()
        # 使用NodeState中的方法创建创世QC，确保一致性
        first_node_state = list(self.nodes.values())[0].state if self.nodes else None
        if first_node_state:
            self.genesis_qc = first_node_state.locked_qc  # 使用NodeState中已经创建的创世QC
            logger.info(f"Using genesis QC from NodeState: view={self.genesis_qc.view}")
        else:
            # 备用：自己创建创世QC
            self.genesis_qc = self._create_genesis_qc()

         # 初始化组件
        self.validator = ProposalValidator(f, nodes, self.genesis_block)
        self.qc_verifier = QCVerifier(f, self.gpk)
        self.vote_manager = VoteManager()
        
        # 为每个节点初始化状态（确保一致）
        for node_id, node in self.nodes.items():
            # 确保节点有state属性
            if not hasattr(node, 'state') or node.state is None:
                node.state = NodeState(node_id=node_id, f=f)
                logger.info(f"Initialized state for node {node_id}")
            
            # === 关键：正确设置节点的创世状态 ===
            try:
                # 1. 添加创世区块到节点状态
                if not node.state.get_block(self.genesis_block.id):
                    node.state.add_block(self.genesis_block)
                    logger.info(f"Added genesis block to node {node_id}")
                
                # 2. 确保节点有创世QC（NodeState初始化时已经创建）
                # 我们只需要确认节点的locked_qc和latest_qc是创世QC
                if node.state.locked_qc.view != 0:
                    logger.warning(f"Node {node_id} locked_qc view is {node.state.locked_qc.view}, not 0")
                    node.state.locked_qc = self.genesis_qc
                
                if node.state.latest_qc.view != 0:
                    logger.warning(f"Node {node_id} latest_qc view is {node.state.latest_qc.view}, not 0")
                    node.state.latest_qc = self.genesis_qc
                
                # 3. 设置节点的高度为0（NodeState.height属性会自动计算）
                # 4. 设置节点的view为0（通过node.view属性）
                node.view = 0
                
                # 5. 更新节点的共识相关状态
                node.is_leader = False
                
                logger.info(f"Node {node_id} initialized with genesis: height={node.state.height}, "
                        f"locked_qc.view={node.state.locked_qc.view}")
                
            except Exception as e:
                logger.error(f"Failed to initialize node {node_id} with genesis: {e}", exc_info=True)
        
        # 检查节点是否有BLS私钥
        for node_id, node in self.nodes.items():
            if not hasattr(node, 'priv') or node.priv is None:
                logger.warning(f"Node {node_id} has no priv_key, voting will fail")
        
        logger.info(f"[Consensus] Initialized with group_pk={self.gpk is not None}, "
                f"public_key_map_size={len(self.public_key_map)}, nodes={len(self.nodes)}")
        
        # 启动共识线程
        self.consensus_thread = threading.Thread(target=self._run_consensus, daemon=True)
        self.message_handler_thread = threading.Thread(target=self._handle_messages, daemon=True)

    def init_dependencies(self):
        self.mempool = self.services.get('mempool')
        self.nodes = self.services.get('nodes')

    def start(self):
        """启动共识协议"""
        logger.info(f"[Consensus] Starting consensus with view={self.view}, nodes={list(self.nodes.keys())}")
        

        self.active = True
        self.consensus_thread.start()
        self.message_handler_thread.start()
        
        logger.info(f"[Consensus] Threads started: consensus_thread={self.consensus_thread.is_alive()}, "
            f"message_handler_thread={self.message_handler_thread.is_alive()}")
        
        return self.consensus_thread
    
    def stop(self):
        """停止共识协议"""
        self.active = False
        event("consensus_stopped", node_id="system", view=self.view, consensus_type="my")

    ###主循环入口==============================================================================================================
    def _run_consensus(self):
        logger.info(f"[Global][Consensus] Starting global consensus loop")

        # 初始化状态
        self._sync_all_nodes_state()  # 初始同步

        consecutive_failures = 0  # 跟踪连续失败次数
        max_consecutive_failures = 3  # 最多允许连续失败3次
        
        while self.active and self.current_round < self.max_rounds:
            current_view = self.view
            leader_id = self.validator._leader_for_view(self.view)
            
            logger.info(f"[Global][Consensus] Round {self.current_round}: view={current_view}, leader={leader_id}")


            # if self.pacemaker.should_skip_leader(leader_id):
            #     logger.info(f"[Global][Consensus] 跳过性能差的领导者 {leader_id}")
            #     self.view += 1
            #     continue
            
            # 在每个轮次开始时同步状态
            self._sync_all_nodes_state()

            # 1. 同步所有节点的视图号
            for node_id, node in self.nodes.items():
                node.view = current_view
                node.is_leader = (node_id == leader_id)
                logger.debug(f"[Global][Consensus] 设置节点 {node_id}.is_leader={node.is_leader}")
            
            # 检查leader是否存在
            if leader_id not in self.nodes:
                logger.info(f"[Global][Consensus] ERROR: Leader {leader_id} not found in nodes!")
                self._handle_missing_leader(current_view)
                continue
            
            # 设置所有节点的leader状态
            for node_id, node in self.nodes.items():
                node.view = current_view
                node.is_leader = (node_id == leader_id)
                logger.info(f"[Global][Consensus] Setting node {node_id}.is_leader={node.is_leader}")
            
             # 2. 领导者创建并广播提案
            leader_node = self.nodes[leader_id]
            if leader_node.is_leader:
                logger.info(f"[Global][Consensus] Leader {leader_id} creating proposal...")

                # 启动提案超时定时器（领导者监控投票超时）
                # self._start_vote_timeout_timer(leader_node, current_view)
                # logger.info(f"[Global][Consensus] Started vote timeout timer for leader {leader_id} in view {current_view}")
            
                #Leader创建区块
                block = self._create_new_block(leader_node)
                if not block:
                    logger.info(f"[Global][Consensus] Failed to create block for leader {leader_id}")
                    consecutive_failures += 1
                    if consecutive_failures >= max_consecutive_failures:
                        logger.info(f"[Global][Consensus] Too many consecutive failures ({consecutive_failures}), stopping")
                    self.consecutive_timeouts += 1
                    self.view += 1
                    time.sleep(0.1)
                    continue

                self.consecutive_timeouts = 0  # 重置连续超时计数
                
                logger.info(f"[Global][Consensus] Leader {leader_id} created block {block.id[:8]}")
                consecutive_failures = 0
                
                # 2. Leader广播提案到所有副本
                logger.info(f"[Global][Consensus] Leader {leader_id} broadcasting proposal...")
                self._broadcast_proposal(leader_node, block)

                # 设置等待投票状态
                self.waiting_for_votes = True
                self.votes_received.clear()  # 清空之前的投票
                
                # 等待投票收集
                wait_start = time.time()
                while time.time() - wait_start < VOTE_TIMEOUT:
                    if len(self.votes_received) >= 2 * self.f + 1:
                        logger.info(f"[Global][Consensus] 收集到足够投票: {len(self.votes_received)}")

                        # 形成QC
                        qc = self._assemble_qc_from_votes(block.hash, self.votes_received)
                        if qc:
                            # self.pacemaker.adjust_timeout(True)
                            # self.pacemaker.record_leader_performance(leader_id, True)
                            # 广播NEW-VIEW
                            self._broadcast_new_view(leader_node, qc)

                            # 更新所有节点状态
                            self._update_all_nodes_with_qc(qc, block)

                            # 清空状态
                            self.votes_received.clear()
                            self.waiting_for_votes = False
                            
                            # 重置连续超时计数
                            self.consecutive_timeouts = 0
                            break
                        # else:
                        #     self.pacemaker.adjust_timeout(False)
                        #     self.pacemaker.record_leader_performance(leader_id, False)

                    time.sleep(0.1)  # end if 
                
                # 检查是否超时
                # if self.waiting_for_votes:
                #     logger.warning(f"[Global][Consensus] 领导者 {leader_id} 投票收集超时")
                    # 超时处理已在定时器回调中完成
            
            else:
                # 3. 副本节点：启动提案接收超时定时器
                self._start_proposal_timeout_timer(self.nodes[node_id], current_view)
        
           
           # 4.1 再次同步状态（确保所有节点跟上）
            self._sync_all_nodes_state()

            # 4. 检查是否有待处理的视图切换
            if self._has_pending_view_change(current_view):
                logger.info(f"[Global][Consensus] 有待处理的视图切换，优先处理")
                # 视图切换会在消息处理中自动执行
            
            # 5. 进入下一轮
            time.sleep(0.5)  # 视图间间隔
            self.current_round += 1

        logger.info("[Global][Consensus] Consensus loop completed")


    def handle_vote(self, node: 'Node', vote_data: dict):
        """处理投票（由Node调用）"""
        # 1. 提取数据
        vote = vote_data.get("vote")
        sender_id = vote_data.get("sender")

         # 如果vote是字典，反序列化为Vote对象
        if isinstance(vote, dict):
            vote = Vote.from_dict(vote)
            if vote is None:
                logger.error(f"[Consensus] handle_vote中vote is none")
        else:
            logger.info(f"[Consensus] vote_data do not need from_dict")
            pass
        
        if vote is None:
            logger.error(f"[Consensus] vote_data中vote字段为空或不存在")
            return False
    
        if sender_id is None:
            logger.error(f"[Consensus] vote_data中sender字段为空或不存在")
            return False

        logger.info(f"[Consensus] vote_data start validation")

        # 2. 验证投票
        if not self._validate_vote(vote, sender_id):
            logger.error(f"[Consensus] Invalid vote from {sender_id}, ignoring")
            return False
        
        # 3. 如果是Leader，收集投票
        if node.is_leader:
            logger.info(f"[Consensus] vote_data is a leader")
            self.votes_received[sender_id] = vote
            logger.info(f"[Consensus] Leader {node.id} 收到来自 {sender_id} 的投票")
            
            # 检查是否收集到足够投票
            if len(self.votes_received) >= 1:  
            #2 * self.f:  # 2f+1
                logger.info(f"[Consensus] start assemble qc")

                 #安全地获取当前提案的区块哈希
                current_block_hash = None

                if hasattr(vote, 'block_hash') and vote.block_hash:
                    current_block_hash = vote.block_hash
                    logger.info(f"[Consensus] 从投票对象获取区块哈希: {current_block_hash.hex()[:8]}")
                    
                    qc = self._assemble_qc_from_votes(current_block_hash, self.votes_received)
                    if qc:
                        # 更新所有节点的状态（通过Node的方法）
                        # 找到对应的区块
                        block = None
                        if hasattr(node, 'state'):
                            block = node.state.get_block_by_hash(current_block_hash)

                        logger.info(f"[Consensus] start update qc state")
                        self._update_all_nodes_state(qc, block)
                        
                        # 广播NEW-VIEW
                        logger.info(f"[Consensus] start broadcast qc ")
                        self._broadcast_new_view(node, qc)
                        
                        # 清空投票收集
                        logger.info(f"[Consensus] clear 4 next round ")
                        self.votes_received.clear()
                else:
                    logger.info(f"[Consensus] 从投票对象获取区块哈希fail!")

        logger.info(f"[Consensus] vote_data end")
        return True
    
    def _update_all_nodes_state(self, qc: QC, block: Block):
        """更新所有节点的状态"""
        for node_id, node in self.nodes.items():
            try:
                node.update_consensus_state(block, qc)
            except Exception as e:
                logger.error(f"更新节点 {node_id} 状态失败: {e}")


    def handle_proposal(self, node: 'Node', proposal_data: dict) -> bool:
        """        
        Args:
            node: 处理提案的节点实例
            proposal_data: 提案数据
            
        Returns:
            bool: 是否接受提案
        """
        logger.info(f"[Consensus] 节点 {node.id} 处理提案")
        
        try:
            # 1. 提取数据
            block_dict = proposal_data.get("block")
            qc_dict = proposal_data.get("qc")
            view = proposal_data.get("view", 0)
            sender_id = proposal_data.get("sender")
            
            if not block_dict or not qc_dict:
                logger.warning(f"[Consensus] 无效提案：缺少block或qc")
                return False
            
            # 2. 反序列化
            from block import Block
            block = Block.from_dict(block_dict) if isinstance(block_dict, dict) else block_dict
            qc = QC.from_dict(qc_dict) if qc_dict and isinstance(qc_dict, dict) else None
            
            if not block or not qc:
                logger.error(f"[Consensus] 反序列化失败")
                return False
            
            # 3. 验证提案
            is_valid, reason = self._validate_proposal_for_replica(node, block, qc, view, sender_id)
            
            if not is_valid:
                logger.warning(f"[Consensus] 提案无效: {reason}")
                
                # 如果怀疑，可以请求Merkle证明
                if self._should_request_proof(node, qc):
                    logger.info(f"[Consensus] 启动Merkle证明验证")
                    proof_valid = self._verify_with_merkle_proof(node, qc, node.index)
                    if not proof_valid:
                        logger.warning(f"[Consensus] Merkle证明验证失败")
                        return False
                
                return False
            
            # 4. 存储当前提案到节点
            node.current_proposal = {
                "block": block,
                "qc": qc,
                "view": view,
                "sender": sender_id,
                "received_at": time.time()
            }
            
            logger.info(f"[Consensus] 节点 {node.id} 提案验证通过")
            
            # 5. 创建并发送投票
            return self._create_and_send_vote_for_replica(node, block, qc, view)
            
        except Exception as e:
            logger.error(f"[Consensus] 处理提案异常: {e}", exc_info=True)
            return False

    def _validate_proposal_for_replica(self, node: 'Node', block, qc: QC, view: int, sender_id: str) -> tuple[bool, str]:
        """
        副本的提案验证        
        Returns:
            (bool, str): (是否有效, 原因)
        """
        logger.info(f"[Consensus] 为节点 {node.id} 验证提案")
        
        # 1. 基础检查
        if view < node.view:
            return False, f"视图 {view} < 当前视图 {node.view}"
        
        # 2. 检查区块有效性
        if not block.validate():
            return False, "区块验证失败"
        
        # 3. 检查提议者是否是当前领导者
        leader_id = self.validator._leader_for_view(view)
        if sender_id != leader_id:
            return False, f"提议者 {sender_id} 不是领导者 {leader_id}"
        
        # 4. 检查QC视图号
        if qc.view >= view:
            return False, f"QC视图 {qc.view} >= 提案视图 {view}"
        
        # 5. 检查父哈希一致性
        # if block.parent_hash != qc.block_hash:
        #     return False, f"区块父哈希 {block.parent_hash.hex()[:8]} != QC区块哈希 {qc.block_hash.hex()[:8]}"
        
        # 6. 分层验证QC
        qc_valid, qc_reason = self._verify_qc_as_replica(node, qc)
        if not qc_valid:
            return False, f"QC验证失败: {qc_reason}"
        
        return True, "验证通过"
    
    def _verify_qc_as_replica(self, node, qc: QC) -> tuple[bool, str]:
        """
        副本的分层QC验证（迁移自Node.verify_qc_as_replica）
        """
        logger.info(f"[Consensus] 节点 {node.id} 验证QC")
        
        # 创世QC的特殊处理
        if qc.view == 0:
            logger.info(f"[Consensus] 创世QC验证通过（特殊处理）")
            return True, "创世QC"
        
        # 1. 基本检查
        if qc.view < node.state.locked_qc.view:
            return False, f"QC视图 {qc.view} < 锁定视图 {node.state.locked_qc.view}"
        
        # 2. 快速位图检查
        signer_count = bin(qc.signer_bitmap).count("1") if hasattr(qc, 'signer_bitmap') else 0
        if signer_count < 2 * node.f + 1:
            return False, f"签名者不足: {signer_count}"
        
        # 3. 检查自己是否在签名者中
        i_am_signer = False
        if hasattr(qc, 'is_signer') and callable(qc.is_signer):
            i_am_signer = qc.is_signer(node.index)
        else:
            # 手动检查
            i_am_signer = (qc.signer_bitmap >> node.index) & 1 == 1
        
        # 4. 如果不在签名者中或怀疑，请求Merkle证明
        if not i_am_signer or self._has_suspicious_qc(node):
            logger.info(f"[Consensus] 节点 {node.id} 需要Merkle证明")
            if not self._verify_with_merkle_proof(node, qc, node.index):
                return False, "Merkle证明验证失败"
        
        # 5. 最终聚合签名验证
        # 这里调用QC验证器
        if hasattr(self, 'qc_verifier'):
            try:
                # 准备验证消息
                message = qc.view.to_bytes(8, 'big') + qc.block_hash
                
                # 使用QC验证器验证
                if self.gpk:
                    # 如果有组公钥，使用组验证
                    if not self.qc_verifier.verify_with_group_pk(qc, self.gpk):
                        return False, "聚合签名验证失败"
                else:
                    # 使用公钥映射验证
                    if not self.qc_verifier.verify_with_pub_keys(qc, self.public_key_map):
                        return False, "聚合签名验证失败"
            except Exception as e:
                logger.error(f"[Consensus] 验证QC签名异常: {e}")
                return False, f"验证异常: {e}"
        else:
            logger.warning(f"[Consensus] 无QC验证器，跳过签名验证")
        
        return True, "QC验证通过"
    
    def _has_suspicious_qc(self, node) -> bool:
        """检查是否有可疑的QC记录"""
        # 这里可以根据节点状态判断
        # 简化：检查是否有失败记录
        if hasattr(node.state, 'suspicious_qc_count'):
            return node.state.suspicious_qc_count > 0
        return False
    
    def _should_request_proof(self, node, qc: QC) -> bool:
        """判断是否需要请求Merkle证明"""
        # 简化逻辑：如果QC视图较高或不在签名者中，需要证明
        if qc.view > node.state.latest_qc.view + 1:
            return True
        
        # 检查是否在签名者中
        if hasattr(qc, 'is_signer') and callable(qc.is_signer):
            return not qc.is_signer(node.index)
        
        return False
    
    def _verify_with_merkle_proof(self, node, qc: QC, replica_index: int) -> bool:
        """
        通过Merkle证明验证QC（迁移自Node.verify_with_merkle_proof）
        """
        logger.info(f"[Consensus] 节点 {node.id} 开始Merkle证明验证")
        
        # 1. 获取领导者ID
        leader_id = self.validator._leader_for_view(qc.view)
        
        # 2. 发送Merkle证明请求
        request_id = self._send_proof_request(node, qc, replica_index, leader_id)
        if not request_id:
            logger.error(f"[Consensus] 发送Merkle证明请求失败")
            return False
        
        # 3. 等待响应（带超时）
        start_time = time.time()
        proof_received = False
        proof = None
        
        while time.time() - start_time < PROOF_REQUEST_TIMEOUT:
            # 检查是否收到证明
            proof_key = (qc.view, qc.block_hash, replica_index)
            if hasattr(node, 'cached_proofs') and proof_key in node.cached_proofs:
                proof = node.cached_proofs.pop(proof_key)
                proof_received = True
                break
            
            time.sleep(0.01)
        
        if not proof_received:
            logger.warning(f"[Consensus] Merkle证明请求超时")
            # 尝试从见证者获取证明
            return self._request_proof_from_witnesses(node, qc, replica_index)
        
        # 4. 验证证明
        if proof:
            return self._verify_merkle_proof(node, proof, qc)
        
        return False
    
    def _send_proof_request(self, node, qc: QC, replica_index: int, leader_id: str) -> Optional[str]:
        """发送Merkle证明请求"""
        try:
            # 生成请求ID
            request_id = hashlib.sha256(
                f"{node.id}_{qc.block_hash.hex()}_{replica_index}_{int(time.time())}".encode()
            ).hexdigest()[:16]
            
            # 发送请求
            self.network.send_proof_request(
                sender_id=node.id,
                request_id=request_id,
                block_hash=qc.block_hash,
                replica_index=replica_index,
                view=qc.view,
                target_id=leader_id
            )
            
            logger.info(f"[Consensus] 节点 {node.id} 发送Merkle证明请求 {request_id} 给 {leader_id}")
            return request_id
            
        except Exception as e:
            logger.error(f"[Consensus] 发送Merkle证明请求异常: {e}")
            return None
    
    def _verify_merkle_proof(self, node, proof: MerkleProof, qc: QC) -> bool:
        """验证Merkle证明"""
        try:
            logger.debug(f"[Consensus] 验证Merkle证明")
            
            # 1. 验证部分签名
            sign_data = proof.view.to_bytes(8, 'big') + proof.block_hash
            
            # 需要获取该副本的公钥
            voter_pk = self.public_key_map.get(proof.voter_id) if hasattr(proof, 'voter_id') else None
            if voter_pk:
                try:
                    from crypto import BLS
                    if not BLS.verify(voter_pk, sign_data, proof.partial_signature):
                        logger.error(f"[Consensus] 部分签名验证失败")
                        return False
                except Exception as e:
                    logger.error(f"[Consensus] 签名验证异常: {e}")
                    return False
            else:
                logger.warning(f"[Consensus] 未找到投票者公钥，跳过部分签名验证")
            
            # 2. 验证Merkle路径
            leaf_hash = proof.calculate_leaf_hash() if hasattr(proof, 'calculate_leaf_hash') else None
            if not leaf_hash:
                # 手动计算叶子哈希
                leaf_data = (
                    proof.replica_index.to_bytes(4, 'big') +
                    proof.partial_signature +
                    proof.view.to_bytes(8, 'big') +
                    proof.block_hash
                )
                leaf_hash = hashlib.sha256(leaf_data).digest()
            
            # 使用MerkleUtils计算根哈希
            from merkle_utils import MerkleUtils
            computed_root = MerkleUtils.compute_root_from_proof(
                leaf_hash, 
                proof.sibling_hashes if hasattr(proof, 'sibling_hashes') else []
            )
            
            if computed_root != qc.merkle_root:
                logger.error(f"[Consensus] Merkle根不匹配: 计算 {computed_root.hex()[:8]} != QC {qc.merkle_root.hex()[:8]}")
                return False
            
            logger.info(f"[Consensus] Merkle证明验证成功")
            return True
            
        except Exception as e:
            logger.error(f"[Consensus] 验证Merkle证明异常: {e}", exc_info=True)
            return False
    
    def _request_proof_from_witnesses(self, node, qc: QC, replica_index: int, k=2) -> bool:
        """向见证者副本请求证明（备用方案）"""
        logger.info(f"[Consensus] 节点 {node.id} 尝试从见证者获取证明")
        
        # 找出可能存有证明的其他副本
        witnesses = []
        for node_id, other_node in self.nodes.items():
            if node_id != node.id and node_id != qc.view % len(self.nodes):
                witnesses.append(node_id)
        
        # 随机选择k个见证者
        import random
        selected_witnesses = random.sample(witnesses, min(k, len(witnesses)))
        
        for witness_id in selected_witnesses:
            logger.info(f"[Consensus] 向见证者 {witness_id} 请求证明")
            # 这里可以发送请求给见证者
            # 简化：返回False
            pass
        
        return False
    
    def _create_and_send_vote_for_replica(self, node, block, qc: QC, view: int) -> bool:
        """副本创建并发送投票"""
        try:
            # 创建投票
            vote = self.vote_manager.create_vote(node, block, view)
            if not vote:
                logger.error(f"[Consensus] 节点 {node.id} 创建投票失败")
                return False
            
            # 发送投票给领导者
            leader_id = self.validator._leader_for_view(view)
            logger.info(f"[Consensus] 节点 {node.id} 发送投票给领导者 {leader_id}")
            
            # 使用vote_manager广播投票
            logger.info(f"[Consensus] 准备广播投票，节点: {node.id}")
            logger.info(f"[Consensus] network实例: {self.network}, vote_manager: {self.vote_manager}")
            success = self.vote_manager.broadcast_vote(
                self.network,
                node.id,
                vote,
                block
            )
            
            if success:
                logger.info(f"[Consensus] 节点 {node.id} 投票发送成功")
                
                # 记录投票状态
                node.last_vote = {
                    "vote": vote,
                    "block_id": block.id,
                    "view": view,
                    "timestamp": time.time()
                }
                
                return True
            else:
                logger.error(f"[Consensus] 节点 {node.id} 发送投票失败")
                return False
                
        except Exception as e:
            logger.error(f"[Consensus] 节点 {node.id} 创建或发送投票异常: {e}")
            return False


    def handle_new_view(self, node: 'Node', payload) -> bool:
        """
        处理NEW-VIEW消息        
        Args:
            node: 处理NEW-VIEW消息的节点
            payload: 消息内容
            
        Returns:
            bool: 是否成功处理
        """
        try:
            sender = payload.get("sender")
            qc_dict = payload.get("qc")
            view = payload.get("view", 0)
            
            logger.info(f"[Consensus] 节点 {node.id} 收到NEW-VIEW消息，来自 {sender}，视图={view}")
            
            if not qc_dict:
                logger.warning(f"[Consensus] 无效的NEW-VIEW: 缺少QC")
                return False
            
            # 反序列化QC
            qc = QC.from_dict(qc_dict) if isinstance(qc_dict, dict) else qc_dict
            
            if not qc:
                logger.error(f"[Consensus] QC反序列化失败")
                return False
            
            # 验证NEW-VIEW消息中的QC
            if not self._validate_new_view_qc(node, qc, sender, view):
                logger.warning(f"[Consensus] NEW-VIEW QC验证失败")
                return False
            
            # 更新状态
            success = self._update_state_with_new_qc(node, qc)
            
            if success:
                logger.info(f"[Consensus] 节点 {node.id} 成功处理NEW-VIEW，视图更新为 {qc.view}")
                return True
            else:
                logger.warning(f"[Consensus] 节点 {node.id} 更新状态失败")
                return False
            
        except Exception as e:
            logger.error(f"[Consensus] 处理NEW-VIEW异常: {e}", exc_info=True)
            return False

    def _update_state_with_new_qc(self, node, qc: QC) -> bool:
        """
        用新的QC更新状态
        复用NodeState中的方法
        """
        try:
            logger.info(f"[Consensus] 节点 {node.id} 用新QC更新状态: view={qc.view}")
            
            # 1. 首先找到对应的区块（如果存在）
            block = node.state.get_block_by_hash(qc.block_hash)
            
            if block:
                # 如果有对应区块，使用带区块的更新
                if node.state.update_latest_qc(qc, block):
                    logger.info(f"[Consensus] 节点 {node.id} 更新latest_qc到视图 {qc.view}，附带区块")
                else:
                    logger.warning(f"[Consensus] 节点 {node.id} 更新latest_qc失败")
            else:
                # 如果没有区块，只更新QC
                # 这通常发生在刚收到NEW-VIEW但还没有收到对应区块时
                if node.state.update_latest_qc(qc, None):
                    logger.info(f"[Consensus] 节点 {node.id} 更新latest_qc到视图 {qc.view}，无区块")
                else:
                    logger.warning(f"[Consensus] 节点 {node.id} 更新latest_qc失败")
            
            # 2. 检查是否需要更新locked_qc
            if self._should_update_locked_qc(node, qc):
                if node.state.update_locked_qc(qc):
                    logger.info(f"[Consensus] 节点 {node.id} 更新locked_qc到视图 {qc.view}")
                else:
                    logger.warning(f"[Consensus] 节点 {node.id} 更新locked_qc失败")
            
            
            # 4. 更新节点的视图号
            if qc.view > node.view:
                old_view = node.view
                node.view = qc.view
                logger.info(f"[Consensus] 节点 {node.id} 更新视图: {old_view} -> {node.view}")
            
            # 5. 检查节点是否成为下一个Leader
            self._check_and_become_next_leader(node, qc.view)
            
            return True
            
        except Exception as e:
            logger.error(f"[Consensus] 更新状态失败: {e}")
            return False

    def _check_and_become_next_leader(self, node, qc_view: int):
        """
        检查节点是否成为下一个Leader
        如果是，准备创建下一个提案
        """
        # 下一个视图是当前QC视图 + 1
        next_view = qc_view + 1
        next_leader_id = self.validator._leader_for_view(next_view)
        
        logger.info(f"[Consensus] 检查下一个领导者: 视图 {next_view} 的领导者是 {next_leader_id}")
        
        if node.id == next_leader_id:
            logger.info(f"[Consensus] 节点 {node.id} 将成为下一个领导者（视图 {next_view}）")
            
            # 标记为领导者（主循环会检查这个状态）
            node.is_leader = True
            
            # 可以在这里触发创建提案，或者让主循环处理
            # 我们选择让主循环处理，所以只是标记状态
        else:
            # 如果不是领导者，确保标记为False
            node.is_leader = False

    def _validate_new_view_qc(self, node, qc: QC, sender: str, view: int) -> bool:
        """
        验证NEW-VIEW消息中的QC
        复用已有的QC验证逻辑
        """
        logger.info(f"[Consensus] 验证NEW-VIEW QC")
        
        # 1. 检查视图号 - NEW-VIEW消息应该携带前一个视图的QC
        if qc.view != view - 1:
            logger.warning(f"[Consensus] QC视图 {qc.view} 与期望的视图 {view-1} 不匹配")
        
        # 2. **复用提案验证中的QC验证逻辑**
        # 注意：这里我们复用 _verify_qc_as_replica 方法
        qc_valid, reason = self._verify_qc_as_replica(node, qc)
        if not qc_valid:
            logger.warning(f"[Consensus] NEW-VIEW QC验证失败: {reason}")
            return False
        
        # 3. 检查QC是否比当前最新QC更新
        if qc.view <= node.state.latest_qc.view:
            logger.debug(f"[Consensus] NEW-VIEW QC视图 {qc.view} <= 当前最新视图 {node.state.latest_qc.view}")
        
        return True
    

    def _broadcast_new_view(self, leader_node, qc: QC):
        """广播NEW-VIEW消息（携带新QC）"""
        self.network.broadcast_new_view(
            sender_id=leader_node.id,
            qc=qc
        )
        
        event("new_view_broadcast", node_id=leader_node.id, view=self.view, 
              consensus_type="my")


    def _create_new_block(self, leader_node) -> Optional[Block]:
        """创建新区块"""
        try:
            # 获取当前高度（从区块树中获取，不是从latest_qc）
            current_height = leader_node.state.height
            
            # 总是使用最新QC作为父QC
            if leader_node.state.latest_qc:
                parent_qc = leader_node.state.latest_qc
                parent_block = leader_node.state.get_block_by_hash(parent_qc.block_hash)
                logger.info(f"[Consensus] Latest QC found: view={parent_qc.view}, parent_block={parent_qc.block_hash[:8].hex()}")
                if parent_block:
                    parent_hash = parent_block.hash
                    next_height = parent_block.height + 1
                else:
                    # 如果没有找到对应区块，使用创世区块
                    logger.warning(f"[Consensus] Parent block for latest_qc not found, using genesis")
                    parent_hash = self.genesis_block.hash
                    parent_qc = self.genesis_qc
                    next_height = 1
            else:
                # 没有latest_qc，使用创世区块
                parent_hash = self.genesis_block.hash
                parent_qc = self.genesis_qc
                next_height = 1
            
            logger.info(f"[Consensus] Creating block at height {next_height}, "
                    f"parent height={current_height}, "
                    f"parent QC view={parent_qc.view}")
            
            # # === 根据高度决定父区块和父QC ===
            # if next_height == 1:
            #     # 高度为1的区块：父区块是创世区块
            #     parent_hash = self.genesis_block.hash
            #     parent_qc = self.genesis_qc
            #     logger.info(f"[Consensus] Creating height=1 block (child of genesis)")
            # else:
            #     # 高度>1的区块：使用最新的QC
            #     parent_hash = leader_node.state.latest_qc.block_hash
            #     if parent_hash is None:
            #         logger.error(f"[Consensus] Cannot create new block: parent_hash is None")
            #         return None
            #     parent_qc = leader_node.state.latest_qc
            
            logger.info(f"[Consensus] Leader {leader_node.id} creating new block at height {next_height}")
            logger.info(f"[Consensus] Parent hash: {parent_hash[:8].hex() if parent_hash else 'None'}")
            logger.info(f"[Consensus] Parent QC view: {parent_qc.view if parent_qc else 'None'}")
            
            # 从内存池获取交易
            if hasattr(self, 'mempool'):
                txs = self.mempool.get_txs(TXS_PER_PROPOSAL)
            else:
                txs = []
                logger.warning("No mempool available, using empty transaction list")
            
            logger.info(f"[Consensus] Transactions for new block: {len(txs)} transactions")
            
            # 创建新区块
            block = Block(
                parent_hash=parent_hash,
                height=next_height,
                proposer=leader_node.id,
                payload=txs,
                qc=parent_qc,  # 指向父区块的QC
                view=self.view
            )

            # logging.info(
            #     f" jdsjhsida: "
            #     f"type={type(block)}, "
            #     f"content={block if isinstance(block, dict) else 'not dict'}"
            # )

            # logger.info(f"Block.hash: {block.hash}")
            # logger.info(f"Block.hash type: {type(block.hash)}")
            # logger.info(f"Block.id: {block.id}")
            
            
            logger.info(f"[Consensus] New block created: id={block.id[:8]}, "
                    f"height={block.height}, view={block.view}")
            
            # 添加到本地状态
            leader_node.state.add_block(block)
            
            return block
            
        except Exception as e:
            error_msg = f"[Consensus] Block creation failed (leader={leader_node.id}, view={self.view}): {str(e)}"
            logger.error(error_msg, exc_info=True)
            return None


    def _broadcast_proposal(self, leader_node, block):
        """广播提案"""
        self.network.broadcast_proposal(
            sender_id=leader_node.id,
            block=block,
            qc=leader_node.state.latest_qc,
            view=self.view
        )
        
        
        event("proposal_broadcast", node_id=leader_node.id, view=self.view, 
              consensus_type="my")
    
    def _collect_votes(self, block: Block, view: int) -> List[Vote]:
        """收集投票"""
        votes = []
        
        for node_id, node in self.nodes.items():
            if node_id == block.proposer:
                continue  # Leader不投票
            
            # 验证提案
            is_valid, reason = self.validator.validate(node, block, None, view)
            
            if is_valid:
                # 创建投票
                vote = self.vote_manager.create_vote(node, block, view)
                if vote:
                    if vote is None:
                        logger.error(f"节点 {node_id} 创建投票失败")
                    if not isinstance(vote, Vote):
                            logger.error(f"创建的投票不是Vote对象: {type(vote)}")
                            continue
                    else:
                        votes.append(vote)
                        # 广播投票
                        self.vote_manager.broadcast_vote(self.network, node_id, vote, block)
                        logger.info(f"节点 {node_id} 投票成功")
        
        return votes
    
    def _assemble_qc_from_votes(self, block_hash: bytes, votes: Dict[str, Vote]) -> Optional[QC]:

        """组装QC"""
        if len(votes) < 1:  # 简化法定数
            logger.warning(f"投票不足: {len(votes)}")
            return None
        
        # 准备数据
        signer_bitmap = 0
        leaves_data = []
        partial_sigs = []

        # 准备消息（所有投票应对同一消息签名）
        message = self.view.to_bytes(8, 'big') + block_hash
  
        for vote in votes.values():
            if not isinstance(vote, Vote):
                logger.error(f"无效的投票对象: 类型={type(vote)}, 值={vote}")
                continue
                
            # # 从公钥映射中获取投票者的公钥
            # voter_pk = self.public_key_map.get(vote.voter_id)
            # if not voter_pk:
            #     logger.warning(f"无法找到投票者 {vote.voter_id} 的公钥")
            #     continue

            # # 验证投票签名
            # try:
            #     from crypto import BLS
            #     if not BLS.verify(voter_pk, message, vote.partial_signature):
            #         logger.warning(f"投票者 {vote.voter_id} 的签名验证失败")
            #         continue
            # except Exception as e:
            #     logger.error(f"验证投票签名时出错 (voter={vote.voter_id}): {e}")
            #     continue

            # 验证投票的有效性
            validation_result = self._validate_vote(vote, message)
            if not validation_result["valid"]:
                continue
            
            # 更新签名者位图
            signer_bitmap |= (1 << vote.voter_index)
            
            leaf_data = (
                vote.voter_index.to_bytes(4, 'big') +
                vote.partial_signature +
                vote.view.to_bytes(8, 'big') +
                block_hash
            )
            leaves_data.append(leaf_data)
            partial_sigs.append(vote.partial_signature)
        
        # 构建Merkle根
        merkle_root = MerkleUtils.build_root(leaves_data)
        
        # 聚合签名
        try:
            from crypto import BLS
            aggregate_sig = BLS.aggregate_partial_sigs(partial_sigs)

            if self.gpk:
                # 验证聚合签名
                # if not BLS.verify_group_signature(self.gpk, message, aggregate_sig):
                #     logger.error("聚合签名验证失败")
                #     return False
                logger.info("聚合签名验证成功---模拟")
            else:
                logger.warning("无组公钥，跳过聚合签名验证")
        except Exception as e:
            logger.error(f"聚合签名失败: {e}")
            return None
        
        # 创建QC
        qc = QC(
            view=self.view,
            block_hash=block_hash,
            aggregate_signature=aggregate_sig,
            signer_bitmap=signer_bitmap,
            merkle_root=merkle_root
        )
        logger.info(f"QC组装成功: view={self.view}, block_hash={block_hash[:8].hex()}")


        # 创建QC后，更新所有节点的状态
        if qc:
            # 找到对应的区块
            leader_id = self.validator._leader_for_view(self.view)
            leader_node = self.nodes[leader_id]
            block = leader_node.state.get_block_by_hash(block_hash)
            
            if block:
                # 更新所有节点的状态
                for node_id, node in self.nodes.items():
                    try:
                        # 1. 添加区块到节点
                        if not node.state.get_block_by_hash(block_hash):
                            node.state.add_block(block)
                        
                        # 2. 更新QC状态
                        node.state.update_latest_qc(qc, block)
                        
                        # 3. 更新视图号
                        node.view = qc.view
                        
                        logger.debug(f"节点 {node_id} 同步QC: view={qc.view}, height={block.height}")
                    except Exception as e:
                        logger.error(f"节点 {node_id} 同步QC失败: {e}")

        return qc


    def _validate_vote(self, vote: Vote, message: bytes) -> Dict[str, any]:
        """
        验证单个投票的有效性
        
        Args:
            vote: 投票对象
            message: 待验证的消息
        
        Returns:
            Dict包含:
                - valid: bool, 投票是否有效
                - reason: str, 如果无效的原因描述
                - public_key: Optional, 投票者的公钥
        """
        result = {
            "valid": False,
            "reason": "",
            "public_key": None
        }
        
        # # 1. 检查公钥是否存在
        # voter_pk = self.public_key_map.get(vote.voter_id)
        # if not voter_pk:
        #     result["reason"] = f"无法找到投票者 {vote.voter_id} 的公钥"
        #     logger.warning(result["reason"])
        #     return result
        
        # result["public_key"] = voter_pk
        
        # # 2. 验证投票的BLS签名
        # try:
        #     from crypto import BLS
        #     if not BLS.verify(voter_pk, message, vote.partial_signature):
        #         result["reason"] = f"投票者 {vote.voter_id} 的签名验证失败"
        #         logger.warning(result["reason"])
        #         return result
        # except Exception as e:
        #     result["reason"] = f"验证投票签名时出错 (voter={vote.voter_id}): {e}"
        #     logger.error(result["reason"])
        #     return result
        
        # # 3. 检查投票view是否与当前view一致（可选，根据需要添加）
        # if hasattr(self, 'view') and vote.view != self.view:
        #     result["reason"] = f"投票view不匹配: {vote.view} != {self.view}"
        #     logger.warning(result["reason"])
        #     return result
        
        # # 4. 验证投票者索引的有效性
        # if vote.voter_index < 0 or vote.voter_index >= len(self.public_key_map):
        #     result["reason"] = f"投票者索引无效: {vote.voter_index}"
        #     logger.warning(result["reason"])
        #     return result
        
        # # 所有验证通过
        result["valid"] = True
        return result


    def _create_genesis_block(self) -> Block:
        """创建创世区块"""
        # 创建全0的父哈希
        zero_hash = bytes([0] * 32)
        
        # 创世区块
        genesis_block = Block(
            parent_hash=zero_hash,
            height=0,
            proposer="genesis",
            payload=[],  # 空交易列表
            qc=None,  # 创世区块没有QC（或者使用创世QC）
            view=0
        )
        
        # 设置一个固定的创世区块ID（与日志中的一致）
        # genesis_id_hex = "0000000000000000000000000000000000000000000000000000000000000000"
        # genesis_block.id = genesis_id_hex
        # genesis_block.hash = bytes.fromhex(zero_hash)

        genesis_block.hash = zero_hash
        genesis_block.id = zero_hash.hex() 
        
        logger.info(f"创建创世区块: height=0, id={genesis_block.id[:8]}, "
                f"hash={genesis_block.hash[:8].hex()}")
        
        return genesis_block

    def _create_genesis_qc(self) -> QC:
        """创建创世QC（如果需要的话）"""
        # 使用创世区块的哈希
        genesis_hash = self.genesis_block.hash if hasattr(self, 'genesis_block') else bytes([0] * 32)
        
        # 创建一个特殊的QC，view=0，签名者为0
        qc = QC(
            view=0,
            block_hash=genesis_hash,
            aggregate_signature=b"",  # 空签名
            signer_bitmap=0,  # 没有签名者
            merkle_root=bytes([0] * 32)  # 全0的Merkle根
        )
        
        logger.info(f"创建创世QC: view=0, block_hash={genesis_hash[:8].hex()}")
        return qc


    def _handle_messages(self):
        """处理网络消息（后台线程）"""
        while self.active:
            # 在实际实现中，这会从网络消息队列中获取并处理消息
            # 这里简化处理
            time.sleep(0.01)


    def handle_timeout(self, node: 'Node', timeout_data) -> bool:
        """
        处理超时消息
        
        Args:
            node: 收到超时消息的节点
            timeout_data: 超时消息数据
            
        Returns:
            bool: 是否成功处理
        """
        try:
            sender = timeout_data.get("sender")
            timeout_type = timeout_data.get("type")  # "vote", "proposal", "view_change"
            view = timeout_data.get("view", 0)
            reason = timeout_data.get("reason", "")
            
            logger.info(f"[Consensus] 节点 {node.id} 收到超时消息，类型={timeout_type}，发送者={sender}，视图={view}")
            
            # 验证超时消息的发送者
            if sender not in self.nodes:
                logger.warning(f"[Consensus] 未知节点发送的超时消息: {sender}")
                return False
            
            # 根据超时类型处理
            if timeout_type == "vote_timeout":
                return self._handle_vote_timeout(node, sender, view, reason)
            elif timeout_type == "proposal_timeout":
                return self._handle_proposal_timeout(node, sender, view, reason)
            elif timeout_type == "view_change_timeout":
                return self._handle_view_change_timeout(node, sender, view, reason)
            else:
                logger.warning(f"[Consensus] 未知的超时类型: {timeout_type}")
                return False
            
        except Exception as e:
            logger.error(f"[Consensus] 处理超时消息异常: {e}", exc_info=True)
            return False
    
    def _handle_vote_timeout(self, node, sender, view, reason):
        """处理投票超时"""
        logger.warning(f"[Consensus] 节点 {node.id} 处理投票超时: 视图={view}, 原因={reason}")
        
        # 记录失败
        self._record_timeout_failure(sender, "vote")
        
        # 检查是否应该触发视图切换
        if self._should_initiate_view_change(node, view, reason):
            logger.info(f"[Consensus] 投票超时，触发视图切换")
            return self._initiate_view_change(node, view, reason)
        
        return True
    
    def _handle_proposal_timeout(self, node, sender, view, reason):
        """处理提案超时"""
        logger.warning(f"[Consensus] 节点 {node.id} 处理提案超时: 视图={view}, 原因={reason}")
        
        # 记录失败
        self._record_timeout_failure(sender, "proposal")
        
        # 如果是领导者故障，立即触发视图切换
        leader_id = self.validator._leader_for_view(view)
        if sender == leader_id and reason == "leader_unresponsive":
            logger.info(f"[Consensus] 领导者 {leader_id} 无响应，触发视图切换")
            return self._initiate_view_change(node, view, f"领导者 {leader_id} 无响应")
        
        return True
    
    def _handle_view_change_timeout(self, node, sender, view, reason):
        """处理视图切换超时"""
        logger.warning(f"[Consensus] 节点 {node.id} 处理视图切换超时: 视图={view}, 原因={reason}")
        
        # 如果视图切换超时，可能需要重新发起或尝试其他策略
        if view == node.view:
            logger.info(f"[Consensus] 当前视图的视图切换超时，尝试重新发起")
            return self._initiate_view_change(node, view, "视图切换超时")
        
        return True
    
    def _record_timeout_failure(self, node_id, failure_type):
        """记录超时失败"""
        # 更新失败计数
        key = f"{node_id}_{failure_type}"
        if key not in self.node_failure_counts:
            self.node_failure_counts[key] = 0
        self.node_failure_counts[key] += 1
        
        # 如果失败次数过多，标记为可疑节点
        if self.node_failure_counts[key] >= self.max_consecutive_timeouts:
            self.suspected_nodes.add(node_id)
            logger.warning(f"[Consensus] 节点 {node_id} 被标记为可疑节点，{failure_type}失败次数过多")
    
    def _should_initiate_view_change(self, node, view, reason):
        """
        判断是否应该发起视图切换
        
        条件：
        1. 当前是领导者且长时间未收到足够投票
        2. 检测到领导者故障
        3. 连续超时次数过多
        """
        # 检查连续超时次数
        if self.consecutive_timeouts >= self.max_consecutive_timeouts:
            logger.info(f"[Consensus] 连续超时次数达到阈值: {self.consecutive_timeouts}")
            return True
        
        # 检查是否是针对当前视图的超时
        if view == node.view:
            # 如果是领导者且投票超时
            if node.is_leader and "vote" in reason.lower():
                logger.info(f"[Consensus] 领导者投票超时，考虑视图切换")
                return True
            
            # 如果是副本且长时间未收到提案
            if not node.is_leader and "proposal" in reason.lower():
                logger.info(f"[Consensus] 副本长时间未收到提案，考虑视图切换")
                return True
        
        return False

    def handle_view_change(self, node: 'Node', view_change_data) -> bool:
        """
        处理视图切换请求
        
        Args:
            node: 收到视图切换请求的节点
            view_change_data: 视图切换数据
            
        Returns:
            bool: 是否接受视图切换请求
        """
        try:
            sender = view_change_data.get("sender")
            new_view = view_change_data.get("new_view", 0)
            reason = view_change_data.get("reason", "")
            evidence = view_change_data.get("evidence", {})  # 故障证据
            
            logger.info(f"[Consensus] 节点 {node.id} 收到视图切换请求，发送者={sender}，新视图={new_view}，原因={reason}")
            
            # 1. 验证视图切换请求
            if not self._validate_view_change_request(node, sender, new_view, reason, evidence):
                logger.warning(f"[Consensus] 视图切换请求验证失败")
                return False
            
            # 2. 记录视图切换请求
            if new_view not in self.view_change_requests:
                self.view_change_requests[new_view] = {}
            
            # 避免重复请求
            if sender in self.view_change_requests[new_view]:
                logger.debug(f"[Consensus] 重复的视图切换请求，忽略")
                return True
            
            # 3. 存储请求
            request = {
                "sender": sender,
                "new_view": new_view,
                "reason": reason,
                "evidence": evidence,
                "timestamp": time.time(),
                "signature": view_change_data.get("signature", b"")
            }
            
            self.view_change_requests[new_view][sender] = request
            logger.info(f"[Consensus] 节点 {node.id} 记录视图切换请求，当前收集到 {len(self.view_change_requests[new_view])} 个请求")
            
            # 4. 检查是否收集到足够多的视图切换请求
            if len(self.view_change_requests[new_view]) >= 2 * self.f + 1:
                logger.info(f"[Consensus] 收集到足够多的视图切换请求，开始视图切换")
                return self._execute_view_change(node, new_view)
            
            # 5. 如果自己是下一个领导者，可以提前准备
            next_leader = self.validator._leader_for_view(new_view)
            if node.id == next_leader and new_view > node.view:
                logger.info(f"[Consensus] 节点 {node.id} 将是新视图的领导者，开始准备")
                self._prepare_for_new_view(node, new_view)
            
            return True
            
        except Exception as e:
            logger.error(f"[Consensus] 处理视图切换请求异常: {e}", exc_info=True)
            return False
    
    def _validate_view_change_request(self, node, sender, new_view, reason, evidence):
        """验证视图切换请求"""
        # 1. 验证发送者
        if sender not in self.nodes:
            logger.warning(f"[Consensus] 未知节点发起的视图切换请求: {sender}")
            return False
        
        # 2. 验证新视图号
        if new_view <= node.view:
            logger.warning(f"[Consensus] 新视图 {new_view} <= 当前视图 {node.view}")
            return False
        
        # 3. 验证请求理由（可选）
        valid_reasons = ["leader_timeout", "leader_crashed", "leader_byzantine", "network_partition"]
        if reason not in valid_reasons:
            logger.warning(f"[Consensus] 无效的视图切换理由: {reason}")
            # 不立即拒绝，可能是自定义理由
        
        # 4. 验证证据（如果有的话）
        if evidence:
            # 检查证据是否有效
            if not self._validate_view_change_evidence(evidence, sender, new_view):
                logger.warning(f"[Consensus] 视图切换证据无效")
                return False
        
        # 5. 验证签名（如果有的话）
        # 这里可以添加签名验证逻辑
        
        return True
    
    def _validate_view_change_evidence(self, evidence, sender, new_view):
        """验证视图切换证据"""
        try:
            evidence_type = evidence.get("type")
            
            if evidence_type == "timeout_log":
                # 超时日志证据
                timeouts = evidence.get("timeouts", [])
                if len(timeouts) >= self.max_consecutive_timeouts:
                    return True
            
            elif evidence_type == "missing_messages":
                # 缺失消息证据
                missing_count = evidence.get("missing_count", 0)
                expected_count = evidence.get("expected_count", 0)
                
                if expected_count > 0 and missing_count / expected_count > 0.5:
                    return True  # 超过50%的消息缺失
            
            elif evidence_type == "byzantine_behavior":
                # 拜占庭行为证据
                behaviors = evidence.get("behaviors", [])
                if len(behaviors) > 0:
                    return True
            
            # 默认接受没有证据或证据类型未知的请求
            # 在实际系统中可能需要更严格的验证
            return True
            
        except Exception as e:
            logger.error(f"[Consensus] 验证证据异常: {e}")
            return False
    
    def _execute_view_change(self, node, new_view):
        """执行视图切换"""
        try:
            logger.info(f"[Consensus] 节点 {node.id} 开始执行视图切换到 {new_view}")
            
            # 1. 停止当前视图的所有活动
            self._stop_current_view_activities(node)
            
            # 2. 更新视图号
            old_view = node.view
            node.view = new_view
            
            # 3. 更新领导者状态
            new_leader_id = self.validator._leader_for_view(new_view)
            node.is_leader = (node.id == new_leader_id)
            
            logger.info(f"[Consensus] 节点 {node.id} 视图切换完成: {old_view} -> {new_view}")
            logger.info(f"[Consensus] 新领导者: {new_leader_id}，当前节点是领导者: {node.is_leader}")
            
            # 4. 清空视图切换请求（针对这个新视图）
            if new_view in self.view_change_requests:
                del self.view_change_requests[new_view]
            
            # 5. 重置连续超时计数
            self.consecutive_timeouts = 0
            
            # 6. 如果自己是新领导者，开始创建提案
            if node.is_leader:
                logger.info(f"[Consensus] 节点 {node.id} 是新领导者，开始工作")
                # 这里可以触发创建提案的逻辑
                # 在实际实现中，可能需要等待一段时间或等待其他节点同步
                
            # 7. 广播视图切换完成消息
            self._broadcast_view_change_complete(node, new_view)
            
            return True
            
        except Exception as e:
            logger.error(f"[Consensus] 执行视图切换异常: {e}")
            return False
    
    def _stop_current_view_activities(self, node):
        """停止当前视图的所有活动"""
        try:
            # 1. 停止等待投票
            self.waiting_for_votes = False
            
            # 2. 取消所有定时器
            if self.vote_timeout_timer:
                self.vote_timeout_timer.cancel()
                self.vote_timeout_timer = None
            
            if self.proposal_timeout_timer:
                self.proposal_timeout_timer.cancel()
                self.proposal_timeout_timer = None
            
            # 3. 清空投票收集
            self.votes_received.clear()
            
            # 4. 清空当前提案
            if hasattr(node, 'current_proposal'):
                node.current_proposal = None
            
            logger.info(f"[Consensus] 节点 {node.id} 停止当前视图活动")
            
        except Exception as e:
            logger.error(f"[Consensus] 停止当前视图活动异常: {e}")
    
    def _prepare_for_new_view(self, node, new_view):
        """为新视图做准备（如果是下一个领导者）"""
        try:
            logger.info(f"[Consensus] 节点 {node.id} 为新视图 {new_view} 做准备")
            
            # 1. 获取最新的QC
            latest_qc = node.state.latest_qc
            
            # 2. 如果需要，获取缺失的状态
            if latest_qc.view < new_view - 1:
                logger.info(f"[Consensus] 需要同步状态，从视图 {latest_qc.view} 到 {new_view - 1}")
                # 这里可以实现状态同步逻辑
                # 例如，从其他节点获取缺失的区块和QC
            
            # 3. 准备创建第一个提案
            # 领导者可以在视图切换完成后立即创建提案
            self._prepare_first_proposal(node, new_view)
            
        except Exception as e:
            logger.error(f"[Consensus] 新视图准备异常: {e}")
    
    def _prepare_first_proposal(self, node, new_view):
        """准备第一个提案"""
        try:
            # 这里可以准备新视图的第一个区块
            # 在实际实现中，可能需要等待所有节点完成视图切换
            logger.info(f"[Consensus] 节点 {node.id} 准备新视图 {new_view} 的第一个提案")
            
            # 可以设置一个定时器，在视图切换稳定后创建提案
            # 简化实现：立即开始创建
            if node.is_leader:
                # 在主循环中会检查这个状态
                pass
            
        except Exception as e:
            logger.error(f"[Consensus] 准备第一个提案异常: {e}")
    
    def _broadcast_view_change_complete(self, node, new_view):
        """广播视图切换完成消息"""
        try:
            self.network.broadcast_view_change_complete(
                sender_id=node.id,
                new_view=new_view,
                latest_qc=node.state.latest_qc
            )
            logger.info(f"[Consensus] 节点 {node.id} 广播视图切换完成，新视图={new_view}")
        except Exception as e:
            logger.error(f"[Consensus] 广播视图切换完成异常: {e}")


    def _initiate_view_change(self, node: 'Node', current_view, reason):
        """
        发起视图切换
        
        Args:
            node: 发起视图切换的节点
            current_view: 当前视图
            reason: 切换原因
            
        Returns:
            bool: 是否成功发起
        """
        try:
            logger.info(f"[Consensus] 节点 {node.id} 发起视图切换，从视图 {current_view}，原因: {reason}")
            
            # 1. 计算新视图号
            new_view = current_view + 1
            
            # 2. 准备视图切换证据
            evidence = {
                "type": "timeout_log",
                "timeouts": self._collect_timeout_evidence(node),
                "reason": reason,
                "timestamp": time.time()
            }
            
            # 3. 创建视图切换请求
            view_change_request = {
                "sender": node.id,
                "new_view": new_view,
                "reason": reason,
                "evidence": evidence,
                "timestamp": time.time()
            }
            
            # 4. 先处理自己的请求
            self.handle_view_change(node, view_change_request)
            
            # 5. 广播给其他节点
            self.network.broadcast_view_change(
                sender_id=node.id,
                new_view=new_view,
                reason=reason
                # evidence=evidence
            )
            
            logger.info(f"[Consensus] 节点 {node.id} 广播视图切换请求")
            
            return True
            
        except Exception as e:
            logger.error(f"[Consensus] 发起视图切换异常: {e}")
            return False
    
    def _collect_timeout_evidence(self, node):
        """收集超时证据"""
        evidence = []
        
        # 收集连续超时记录
        evidence.append({
            "type": "consecutive_timeouts",
            "count": self.consecutive_timeouts,
            "max_allowed": self.max_consecutive_timeouts
        })
        
        # 收集可疑节点信息
        if self.suspected_nodes:
            evidence.append({
                "type": "suspected_nodes",
                "nodes": list(self.suspected_nodes)
            })
        
        return evidence


    def _start_vote_timeout_timer(self, node, view):
        """启动投票超时定时器"""
        try:
            # 取消现有定时器
            if self.vote_timeout_timer:
                self.vote_timeout_timer.cancel()
            
            # 创建新定时器
            self.vote_timeout_timer = threading.Timer(
                VOTE_TIMEOUT,
                self._on_vote_timeout,
                args=[node, view]
            )
            self.vote_timeout_timer.start()
            
            logger.debug(f"[Consensus] 启动投票超时定时器，视图={view}，超时时间={VOTE_TIMEOUT}s")
            
        except Exception as e:
            logger.error(f"[Consensus] 启动投票超时定时器异常: {e}")
    
    def _on_vote_timeout(self, node, view):
        """投票超时回调"""
        if self.waiting_for_votes and view == node.view:
            logger.warning(f"[Consensus] 投票超时，视图={view}")
            
            # 增加连续超时计数
            self.consecutive_timeouts += 1
            
            # 广播超时消息
            timeout_msg = {
                "sender": node.id,
                "type": "vote_timeout",
                "view": view,
                "reason": f"等待投票超时，已等待{VOTE_TIMEOUT}秒"
            }
            
            self.network.broadcast_timeout(node.id, timeout_msg)
            
            # 如果自己是领导者，考虑发起视图切换
            if node.is_leader:
                self._initiate_view_change(node, view, "vote_timeout")
    
    def _start_proposal_timeout_timer(self, node, view):
        """启动提案超时定时器（副本节点使用）"""
        try:
            # 取消现有定时器
            if self.proposal_timeout_timer:
                self.proposal_timeout_timer.cancel()
            
            # 创建新定时器
            self.proposal_timeout_timer = threading.Timer(
                PROPOSAL_TIMEOUT,
                self._on_proposal_timeout,
                args=[node, view]
            )
            self.proposal_timeout_timer.start()
            
            logger.debug(f"[Consensus] 启动提案超时定时器，视图={view}，超时时间={PROPOSAL_TIMEOUT}s")
            
        except Exception as e:
            logger.error(f"[Consensus] 启动提案超时定时器异常: {e}")
    
    def _on_proposal_timeout(self, node, view):
        """提案超时回调"""
        if view == node.view and not node.is_leader:
            logger.warning(f"[Consensus] 提案超时，视图={view}，长时间未收到领导者提案")
            
            # 增加连续超时计数
            self.consecutive_timeouts += 1
            
            # 广播超时消息
            timeout_msg = {
                "sender": node.id,
                "type": "proposal_timeout",
                "view": view,
                "reason": f"长时间未收到领导者提案"
            }
            
            self.network.broadcast_timeout(node.id, timeout_msg)
            
            # 发起视图切换
            self._initiate_view_change(node, view, "leader_unresponsive")


    def _handle_missing_leader(self, current_view):
        """处理缺失的领导者"""
        logger.warning(f"[Consensus] 视图 {current_view} 的领导者不存在")
        
        # 立即触发视图切换
        # 任意节点都可以发起，这里选择索引最小的节点
        available_nodes = [node for node_id, node in self.nodes.items() if node_id in self.nodes]
        if available_nodes:
            initiator = min(available_nodes, key=lambda n: n.id)
            self._initiate_view_change(initiator, current_view, "leader_not_found")
        
        # 增加视图号，继续尝试
        self.view += 1
        time.sleep(1.0)  # 给视图切换一些时间
    
    def _has_pending_view_change(self, current_view):
        """检查是否有待处理的视图切换"""
        # 检查是否有针对下一个视图的切换请求
        next_view = current_view + 1
        if next_view in self.view_change_requests:
            request_count = len(self.view_change_requests[next_view])
            if request_count >= 2 * self.f + 1:
                return True
        return False


    def _update_all_nodes_with_qc(self, qc: QC, block: Block):
        """
        用新QC更新所有节点的状态
        现在主要依赖于 NodeState 的内部逻辑
        """
        logger.info(f"[Consensus] 开始用QC更新所有节点: view={qc.view}, block={block.id[:8]}")
        
        # 1. 验证QC和区块的基本关联
        if not self._validate_qc_block_basic(qc, block):
            logger.error(f"[Consensus] QC和区块基本验证失败")
            return False
        
        # 2. 统计更新结果
        success_count = 0
        failed_nodes = []
        
        for node_id, node in self.nodes.items():
            try:
                # 调用统一的节点状态更新方法
                update_success = self._update_node_state_with_qc_block(node, qc, block)
                
                if update_success:
                    success_count += 1
                    logger.debug(f"[Consensus] 节点 {node_id} 状态更新成功")
                else:
                    failed_nodes.append(node_id)
                    logger.warning(f"[Consensus] 节点 {node_id} 状态更新失败")
                    
            except Exception as e:
                failed_nodes.append(node_id)
                logger.error(f"[Consensus] 更新节点 {node_id} 状态异常: {e}")
        
        # 3. 统计和记录结果
        total_nodes = len(self.nodes)
        logger.info(f"[Consensus] QC状态更新完成: 成功={success_count}/{total_nodes}")
        
        # 4. 检查是否达到法定数
        if success_count >= 2 * self.f + 1:
            logger.info(f"[Consensus] QC被多数节点接受 (>= {2 * self.f + 1} 个节点)")
            
            # 通知所有节点QC已被多数接受
            self._notify_qc_accepted(qc, block, success_count)
            
            return True
        else:
            logger.warning(f"[Consensus] QC未达到法定数: {success_count} < {2 * self.f + 1}")
            return False
    
    def _update_node_state_with_qc_block(self, node, qc: QC, block: Block) -> bool:
        """
        更新单个节点的状态（使用 NodeState 的统一接口）
        
        步骤：
        1. 确保区块存在
        2. 更新 latest_qc
        3. NodeState 内部会自动处理 locked_qc 和提交逻辑
        4. 更新节点视图
        """
        try:
            logger.debug(f"[Consensus] 更新节点 {node.id} 状态: QC view={qc.view}, block height={block.height}")
            
            # 1. 确保区块存在（如果不存在则添加）
            existing_block = node.state.get_block_by_hash(block.hash)
            if not existing_block:
                logger.info(f"[Consensus] 节点 {node.id} 添加新区块 {block.id[:8]}")
                if not node.state.add_block(block):
                    logger.warning(f"[Consensus] 节点 {node.id} 添加区块失败")
                    return False
            
            # 2. 更新 latest_qc（NodeState 会处理后续逻辑）
            # 注意：这里调用的是 NodeState 的 update_latest_qc 方法
            old_qc_view = node.state.latest_qc.view
            if not node.state.update_latest_qc(qc, block):
                logger.warning(f"[Consensus] 节点 {node.id} 更新 latest_qc 失败")
                return False
            
            logger.info(f"[Consensus] 节点 {node.id} 更新 latest_qc: {old_qc_view} -> {qc.view}")
            
            # 3. NodeState 内部会自动检查是否更新 locked_qc 和触发提交
            # 我们可以检查一下 locked_qc 是否有更新
            locked_view = node.state.locked_qc.view if node.state.locked_qc else 0
            logger.debug(f"[Consensus] 节点 {node.id} 当前 locked_qc.view={locked_view}")
            
            # 4. 更新节点视图号（与QC视图保持一致）
            if qc.view > node.view:
                old_view = node.view
                node.view = qc.view
                logger.debug(f"[Consensus] 节点 {node.id} 更新视图: {old_view} -> {node.view}")
            
            # 5. 记录统计信息
            if hasattr(node, 'metrics'):
                # node.metrics.record_event("qc_processed", node.id, {
                #     "qc_view": qc.view,
                #     "block_height": block.height,
                #     "timestamp": time.time()
                # })
                pass
            
            # 6. 检查提交状态（可选，NodeState 已处理）
            self._log_commit_status(node)
            
            return True
            
        except Exception as e:
            logger.error(f"[Consensus] 更新节点 {node.id} 状态异常: {e}", exc_info=True)
            return False

    def _validate_qc_block_basic(self, qc: QC, block: Block) -> bool:
        """基本验证：确保QC和区块关联"""
        try:
            # 1. 检查QC的区块哈希是否与区块匹配
            if qc.block_hash != block.hash:
                logger.error(f"[Consensus] QC区块哈希不匹配")
                return False
            
            # 2. 检查QC视图和区块视图
            if qc.view != block.view:
                logger.warning(f"[Consensus] QC视图 {qc.view} 与区块视图 {block.view} 不一致")
                # 这不一定是错误，但记录警告
            
            return True
            
        except Exception as e:
            logger.error(f"[Consensus] 验证QC-区块异常: {e}")
            return False

    def _log_commit_status(self, node):
        """记录节点的提交状态（用于调试）"""
        try:
            committed_height = getattr(node.state, 'committed_height', 0)
            latest_block = node.state.get_chain_head()
            latest_height = latest_block.height if latest_block else 0
            
            logger.debug(f"[Consensus] 节点 {node.id} 提交状态: "
                        f"已提交高度={committed_height}, "
                        f"最新高度={latest_height}, "
                        f"locked_qc.view={node.state.locked_qc.view}")
            
        except Exception as e:
            logger.debug(f"[Consensus] 记录提交状态异常: {e}")
    
    def _notify_qc_accepted(self, qc: QC, block: Block, accept_count: int):
        """通知所有节点QC已被多数接受"""
        try:
            # 这个通知是可选的，用于统计和监控
            logger.info(f"[Consensus] QC view={qc.view} 已被 {accept_count} 个节点接受")
            
            # 如果需要，可以广播一个轻量级的确认消息
            # 在实际系统中，这有助于节点确认QC的最终性
            self._broadcast_qc_acceptance_notification(qc, block, accept_count)
            
        except Exception as e:
            logger.error(f"[Consensus] 通知QC接受异常: {e}")
    
    def _broadcast_qc_acceptance_notification(self, qc: QC, block: Block, accept_count: int):
        """广播QC接受通知（可选）"""
        try:
            # 这是一个轻量级的通知，不是协议必需部分
            # 主要用于监控、统计和调试
            self.network.broadcast_qc_acceptance(
                sender_id=self._get_current_leader_id(),
                qc_view=qc.view,
                block_id=block.id,
                accept_count=accept_count,
                timestamp=time.time()
            )
            
            logger.debug(f"[Consensus] 广播QC接受通知: view={qc.view}")
            
        except Exception as e:
            logger.debug(f"[Consensus] 广播QC接受通知异常: {e}")
    
    def _get_current_leader_id(self) -> str:
        """获取当前领导者ID"""
        return self.validator._leader_for_view(self.view)


    def _sync_all_nodes_state(self):
        """
        同步所有节点的状态
        
        同步内容：
        1. 视图号 (view)
        2. 领导者状态 (is_leader)
        3. 最新的QC (optional)
        4. 其他共识相关状态
        """
        current_view = self.view
        leader_id = self.validator._leader_for_view(current_view)
        
        logger.debug(f"[Consensus] 同步节点状态: view={current_view}, leader={leader_id}")
        
        synced_count = 0
        for node_id, node in self.nodes.items():
            try:
                # 1. 同步视图号
                if node.view != current_view:
                    old_view = node.view
                    node.view = current_view
                    logger.debug(f"[Consensus] 节点 {node_id} 视图同步: {old_view} -> {current_view}")
                
                # 2. 同步领导者状态
                is_leader = (node_id == leader_id)
                if node.is_leader != is_leader:
                    node.is_leader = is_leader
                    logger.debug(f"[Consensus] 节点 {node_id} 领导者状态: {node.is_leader}")
                
                # 3. 可选：同步最新QC（确保所有节点都有最新QC）
                self._sync_latest_qc_if_needed(node)
                
                # 4. 可选：同步区块（确保所有节点都有必要区块）
                self._sync_missing_blocks(node)
                
                synced_count += 1
                
            except Exception as e:
                logger.error(f"[Consensus] 同步节点 {node_id} 状态失败: {e}")
        
        logger.info(f"[Consensus] 状态同步完成: {synced_count}/{len(self.nodes)} 个节点已同步")
        return synced_count
    
    def _sync_latest_qc_if_needed(self, node):
        """如果节点没有最新QC，尝试同步"""
        try:
            # 获取全局最新QC（例如从领导者）
            global_latest_qc = self._get_global_latest_qc()
            
            if global_latest_qc and global_latest_qc.view > node.state.latest_qc.view:
                logger.info(f"[Consensus] 节点 {node.id} QC落后: {node.state.latest_qc.view} -> {global_latest_qc.view}")
                
                # 可以在这里触发QC同步
                # 简化：记录需要同步，实际同步可以由节点请求
                if hasattr(node, 'needs_qc_sync'):
                    node.needs_qc_sync = global_latest_qc
                
        except Exception as e:
            logger.debug(f"[Consensus] 检查QC同步失败: {e}")
    
    def _sync_missing_blocks(self, node):
        """同步缺失的区块"""
        try:
            # 获取节点的最高区块
            node_head = node.state.get_chain_head()
            if not node_head:
                return
            
            # 获取全局最高区块
            global_head = self._get_global_chain_head()
            if not global_head:
                return
            
            # 如果节点落后，记录需要同步
            if global_head.height > node_head.height + 1:
                logger.info(f"[Consensus] 节点 {node.id} 区块落后: {node_head.height} -> {global_head.height}")
                
                # 标记需要区块同步
                if hasattr(node, 'needs_block_sync'):
                    node.needs_block_sync = {
                        "from_height": node_head.height + 1,
                        "to_height": global_head.height
                    }
                
        except Exception as e:
            logger.debug(f"[Consensus] 检查区块同步失败: {e}")
    
    def _get_global_latest_qc(self):
        """获取全局最新QC（通常是领导者的QC）"""
        # 简单实现：返回领导者节点的最新QC
        leader_id = self.validator._leader_for_view(self.view)
        if leader_id in self.nodes:
            return self.nodes[leader_id].state.latest_qc
        return None
    
    def _get_global_chain_head(self):
        """获取全局链头"""
        # 简单实现：返回领导者节点的链头
        leader_id = self.validator._leader_for_view(self.view)
        if leader_id in self.nodes:
            return self.nodes[leader_id].state.get_chain_head()
        return None