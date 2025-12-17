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
from consensus import Consensus
from block import Block

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
        self.consensus = Consensus(nodes={}, network=network, f=f)
        
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
        self.cached_proofs = {}  # {(view, block_hash, replica_index): MerkleProof}

        # 当前处理中的提案
        self.current_proposal = None
        self.last_proposal = None
        self.last_merkle_proof = None
        
        # 定时器
        self._soft_timer = None
        self._defer_timer = None
        self._view_timer = None
        
         # 消息处理线程
        self.message_queue = []
        self.message_lock = threading.Lock()
        self.processing_active = True
        self.processing_thread = threading.Thread(target=self._process_message_queue, daemon=True)
        self.processing_thread.start()

        logger.info(f"节点 {self.id} 初始化完成，索引={index}")
        
    
    # ==================== 消息处理（更新） ====================
    
    def receive_message(self, msg_type, payload):
        logger.info("处理所有类型的消息")
        # self.consensus.on_message(msg_type, payload)

        """处理所有类型的消息"""
        sender = payload.get("sender", "unknown")
        
         if msg_type == self.network.MSG_TYPES["PROPOSAL"]:
            return self.consensus.handle_proposal(self, payload)
        elif msg_type == self.network.MSG_TYPES["VOTE"]:
            return self.consensus.handle_vote(self, payload)
        elif msg_type == self.network.MSG_TYPES["NEW_VIEW"]:
            return self.consensus.handle_new_view(self, payload)
        elif msg_type == self.network.MSG_TYPES["TIMEOUT"]:
            return self.consensus.handle_timeout(self, payload)
        elif msg_type == self.network.MSG_TYPES["VIEW_CHANGE"]:
            return self.consensus.handle_view_change(self, payload)
        elif msg_type == self.network.MSG_TYPES["REQUEST_MERKLE_PROOF"]:
            return self.handle_proof_request(payload)  # Node 直接处理
        elif msg_type == self.network.MSG_TYPES["MERKLE_PROOF"]:
            return self.handle_proof_response(payload)  # Node 直接处理
        else:
            logger.warning(f"[{self.id}] 未知消息类型: {msg_type}")
    
    # def handle_proposal(self, proposal_data: dict) -> bool:
    #     """处理提案"""
    #     # 反序列化提案
    #     block = Block.from_dict(proposal_data.get("block"))
    #     qc = QC.from_dict(proposal_data.get("qc"))
    #     view = proposal_data.get("view")
    #     sender = proposal_data.get("sender")
        
    #     # 存储提案
    #     self.current_proposal = {
    #         "block": block,
    #         "qc": qc,
    #         "view": view,
    #         "sender": sender
    #     }
        
    #     # 这里只存储，实际验证和投票由共识层协调
    #     logger.info(f"节点 {self.id} 收到提案，视图={view}")
    #     return True
    
    # def handle_new_view(self, qc: QC):
    #     """处理NEW-VIEW消息"""
    #     # 更新状态
    #     if qc.view > self.state.latest_qc.view:
    #         self.state.update_latest_qc(qc, None)
    #         logger.info(f"节点 {self.id} 更新QC到视图 {qc.view}")

    def handle_proof_request(self, request_data: dict):
        """处理Merkle证明请求"""
        try:
            request_id = request_data.get("request_id")
            block_hash = request_data.get("block_hash")
            replica_index = request_data.get("replica_index")
            view = request_data.get("view")
            
            logger.info(f"节点 {self.id} 收到Merkle证明请求: {request_id}")
            
            # 检查是否有所需的区块
            block = self.state.get_block_by_hash(block_hash)
            if not block:
                logger.warning(f"节点 {self.id} 没有区块 {block_hash[:8].hex()}")
                return False
            
            # 生成Merkle证明
            merkle_proof = self._generate_merkle_proof(block, replica_index, view)
            if not merkle_proof:
                return False
            
            # 发送证明回请求者
            self.network.send_merkle_proof(
                sender_id=self.id,
                request_id=request_id,
                merkle_proof=merkle_proof
            )
            
            return True
            
        except Exception as e:
            logger.error(f"处理Merkle证明请求失败: {e}")
            return False
    
    def handle_proof_response(self, response_data: dict):
        """处理Merkle证明响应"""
        try:
            request_id = response_data.get("request_id")
            merkle_proof = response_data.get("merkle_proof")
            
            # 查找对应的请求
            if request_id in self.pending_proof_requests:
                timestamp, callback = self.pending_proof_requests.pop(request_id)
                
                # 验证证明（由共识层或验证器执行）
                is_valid = self.consensus.verify_merkle_proof(merkle_proof)
                
                # 回调处理结果
                if callback:
                    callback(is_valid, merkle_proof)
                    
                return True
            else:
                logger.warning(f"未知的证明请求ID: {request_id}")
                return False
                
        except Exception as e:
            logger.error(f"处理Merkle证明响应失败: {e}")
            return False

    def update_consensus_state(self, block: Block, qc: QC):
        """更新共识状态（由共识层调用）"""
        try:
            # 1. 添加区块
            if not self.state.get_block_by_hash(block.hash):
                self.state.add_block(block)
                logger.info(f"节点 {self.id} 添加区块 {block.id[:8]}, 高度={block.height}")
            
            # 2. 更新QC
            if qc and qc.view > self.state.latest_qc.view:
                old_view = self.state.latest_qc.view
                self.state.update_latest_qc(qc, block)
                logger.info(f"节点 {self.id} 更新QC: 视图 {old_view} -> {qc.view}")
            
            # 3. 更新本地视图号
            if qc and qc.view > self.view:
                old_view = self.view
                self.view = qc.view
                logger.info(f"节点 {self.id} 更新视图: {old_view} -> {self.view}")
            
            # 4. 如果自己是leader，需要检查是否需要创建下一个提案
            if self.is_leader:
                self._check_for_next_proposal()
                
            return True
            
        except Exception as e:
            logger.error(f"节点 {self.id} 更新状态失败: {e}")
            return False