import logging
import time
from typing import Dict, List, Optional, Any
from block import Block
from qc import QC
from network import Network
from votes import Vote
from ProposalValidator import ProposalValidator
from qc_verifier import QCVerifier
from vote_manager import VoteManager
from merkle_utils import MerkleUtils
import threading
from utils.logger import event


logger = logging.getLogger(__name__)

# consensus/
# ├── __init__.py          # 导出共识组件
# ├── consensus_core.py    # 共识主循环和协调逻辑
# ├── proposal_validator.py # 统一的提案验证
# ├── qc_verifier.py       # 统一的QC验证
# ├── vote_manager.py      # 统一的投票管理
# ├── merkle_utils.py      # Merkle树工具
# ├── view_manager.py      # 视图管理
# ├── block_creator.py     # 区块创建
# └── message_handler.py   # 消息处理


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

    
    def _run_consensus(self):
        logger.info("主共识循环：全局协调所有节点")
        logger.info(f"[Consensus] Starting global consensus loop")

        consecutive_failures = 0  # 跟踪连续失败次数
        max_consecutive_failures = 3  # 最多允许连续失败3次
        
        while self.active and self.current_round < self.max_rounds:
            current_view = self.view
            leader_id = self.validator._leader_for_view(self.view)
            
            logger.info(f"[Consensus] Round {self.current_round}: view={current_view}, leader={leader_id}")

            # 1. 同步所有节点的视图号
            for node in self.nodes.values():
                node.view = current_view
                
            # 检查leader是否存在
            if leader_id not in self.nodes:
                logger.info(f"[Consensus] ERROR: Leader {leader_id} not found in nodes!")
                break
            
            # 设置所有节点的leader状态
            for node_id, node in self.nodes.items():
                node.view = current_view
                node.is_leader = (node_id == leader_id)
                logger.info(f"[Consensus] Setting node {node_id}.is_leader={node.is_leader}")
            
            # Leader创建并广播提案
            leader_node = self.nodes[leader_id]
            logger.info(f"[Consensus] Leader {leader_id} creating proposal...")
            
            # 1. Leader创建区块
            block = self._create_new_block(leader_node)
            if not block:
                logger.info(f"[Consensus] Failed to create block for leader {leader_id}")
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    logger.info(f"[Consensus] Too many consecutive failures ({consecutive_failures}), stopping")
                self.view += 1
                time.sleep(0.1)
                continue
            
            logger.info(f"[Consensus] Leader {leader_id} created block {block.id[:8]}")
            consecutive_failures = 0
            
            # 2. Leader广播提案到所有副本
            self._broadcast_proposal(leader_node, block)
            
            # 3. 模拟所有副本接收并投票
            votes = []
            for node_id, node in self.nodes.items():
                if node_id == leader_id:
                    continue  # Leader不给自己投票
                    
                logger.info(f"[Consensus] Node {node_id} processing proposal...")
                
                # 验证提案
                is_valid, reason = self.validator.validate(node, block, None, current_view)

                if is_valid:
                    # 创建投票
                    vote = self.vote_manager.create_vote(node, block, current_view)
                    if vote:
                        votes.append(vote)
                        logger.info(f"[Consensus] Node {node_id} voted for block {block.id[:8]}")

                        block_hash_hex = block.hash
                        if block_hash_hex is None:
                            logger.error(f"[Consensus111] Cannot create vote: block hash is None")
                            return

                        # 发送投票给Leader
                        try:
                            # 使用network.broadcast_vote方法
                            # self.vote_manager.broadcast_vote(
                            #     sender_id=node_id,
                            #     block_id=block.id,  # 区块ID
                            #     view=current_view,
                            #     partial_sig=vote.partial_signature,
                            #     voter_index=node.index,  # 节点索引
                            #     block_hash=block.hash # 区块哈希
                            # )
                            self.vote_manager.broadcast_vote(self.network, node_id, vote, block)

                            logger.debug(f"[Consensus] Vote broadcast from {node_id} to leader {leader_id}")
                        except Exception as e:
                            logger.error(f"[Consensus] Failed to broadcast vote from {node_id}: {e}")
            
            # 4. Leader收集投票并形成QC  todo
            # if len(votes) >= 1 * self.f:
            if len(votes) >= 1:
                logger.info(f"[Consensus] Leader {leader_id} collected {len(votes)} votes, forming QC...")
                qc = self._assemble_qc_from_votes(block.hash, {v.voter_id: v for v in votes})
                
                if qc:
                    # 更新ld的状态
                    # for node in self.nodes.values():
                    #     node.state.update_latest_qc(qc)
                    # leader_node.state.update_latest_qc(qc, block)  # 传入区块
                    node.state.update_latest_qc(qc, block)
  
                    # 广播NEW-VIEW
                    self._broadcast_new_view(leader_node, qc)
                    
                    logger.info(f"[Consensus] QC formed for view {current_view}, block {block.id[:8]}")
                    self.stats["qcs_formed"] += 1
            
            self.stats["proposals_made"] += 1
            self.current_round += 1
            self.view += 1
            
            logger.info(f"[Consensus] Round {self.current_round} completed, advancing to view {self.view}")
            time.sleep(0.5)  # 模拟一轮的时间
        
        logger.info("[Consensus] Consensus loop completed")


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
                
            # 从公钥映射中获取投票者的公钥
            voter_pk = self.public_key_map.get(vote.voter_id)
            if not voter_pk:
                logger.warning(f"无法找到投票者 {vote.voter_id} 的公钥")
                continue

            # 验证投票签名
            try:
                from crypto import BLS
                if not BLS.verify(voter_pk, message, vote.partial_signature):
                    logger.warning(f"投票者 {vote.voter_id} 的签名验证失败")
                    continue
            except Exception as e:
                logger.error(f"验证投票签名时出错 (voter={vote.voter_id}): {e}")
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