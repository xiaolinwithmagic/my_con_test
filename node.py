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
from services import services


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

SOFT_VOTE_TIMEOUT = 0.6
DEFER_TIMEOUT = 1.2
VIEW_TIMEOUT = 3.0
PROOF_REQUEST_TIMEOUT = 1.0

class Node:
    def __init__(self, node_id, index, priv_key, pub_key, network: Network,group_pk,did_priv: int, did_pub, f=1, all_nodes=None,auto_start_threads=True):
        self.id = node_id
        self.index = index  # 新增：节点索引（用于signer_bitmap）
        self.priv = priv_key
        self.pub = pub_key
        self.network = network
        self.f = f
        self.gpk = group_pk
        self.did_priv = did_priv
        self.did_pub = did_pub
        # self.consensus = Consensus(nodes={}, network=network, f=f)
        self.services = None  # 占位符，注册时由 services 自动注入
        self.consensus = None  # 延迟初始化（通过 services 获取）
        
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
        self.processing_active = False
        # self.processing_thread = threading.Thread(target=self._process_message_queue, daemon=True)
        # self.processing_thread.start()
        if auto_start_threads:
            self.start_message_processing()
        
    def _process_message_queue(self):
        """
        消息队列处理线程的主函数
        处理异步接收到的消息
        """
        logger.info(f"[Node] 节点 {self.id} 启动消息队列处理")
        
        while self.processing_active:
            try:
                # 1. 检查是否有消息
                with self.message_lock:
                    if not self.message_queue:
                        # 队列为空，短暂休眠
                        time.sleep(0.01)
                        continue
                    
                    # 2. 取出消息（先进先出）
                    msg_type, payload, received_at = self.message_queue.pop(0)
                
                # 3. 处理消息（记录延迟）
                current_time = time.time()
                network_delay = current_time - received_at
                
                logger.debug(f"[Node] 节点 {self.id} 处理队列消息，类型={msg_type}，"
                           f"延迟={network_delay:.3f}s")
                
                # 4. 调用消息处理器
                result = self._process_single_message(msg_type, payload)
                
                # 5. 记录指标
                # if self.metrics:
                #     self.metrics(
                #         node_id=self.id,
                #         msg_type=msg_type,
                #         delay=network_delay,
                #         success=result
                #     )

        #               def __init__(self):
        # self.lock = threading.Lock()
        # self.tx_create_time = {}   # txid -> create_time
        # self.tx_included_time = {} # txid -> included_in_block_time
        # self.tx_committed_time = {}# txid -> committed_time
        # self.committed_count = 0
                
                # 短暂休眠，避免CPU占用过高
                time.sleep(0.001)
                
            except Exception as e:
                logger.error(f"[Node] 节点 {self.id} 消息处理异常: {e}")
                time.sleep(0.1)  # 异常时稍作等待
        
        logger.info(f"[Node] 节点 {self.id} 消息队列处理停止")
    
    def _process_single_message(self, msg_type: int, payload: dict) -> bool:
        """
        处理单个消息
        调用相应的处理器方法
        """
        try:
            # 根据消息类型调用不同的处理器
            if msg_type == self.network.MSG_TYPES["PROPOSAL"]:
                return self.handle_proposal(payload)
            
            elif msg_type == self.network.MSG_TYPES["VOTE"]:
                return self.handle_vote(payload)
            
            elif msg_type == self.network.MSG_TYPES["NEW_VIEW"]:
                return self.handle_new_view(payload)
            
            # elif msg_type == self.network.MSG_TYPES["TIMEOUT"]:
            #     return self.handle_timeout(payload)
            
            elif msg_type == self.network.MSG_TYPES["VIEW_CHANGE"]:
                return self.handle_view_change(payload)
            
            else:
                # logger.warning(f"[Node] 节点 {self.id} 收到未知消息类型: {msg_type}")
                # return False
                pass
                
        except Exception as e:
            logger.error(f"[Node] 节点 {self.id} 处理消息异常: {e}", exc_info=True)
            return False
    
    def handle_view_change(self, data: dict) -> bool:
        result = self.consensus.handle_view_change(self, data)
        return result

    def enqueue_message(self, msg_type: int, payload: dict):
        """
        将消息加入队列（由Network调用）
        
        Args:
            msg_type: 消息类型
            payload: 消息内容
        """
        try:
            with self.message_lock:
                # 添加消息到队列，记录接收时间
                self.message_queue.append((msg_type, payload, time.time()))
                
                # 限制队列大小，避免内存溢出
                max_queue_size = 1000
                if len(self.message_queue) > max_queue_size:
                    logger.warning(f"[Node] 节点 {self.id} 消息队列过长: {len(self.message_queue)}")
                    # 丢弃最早的消息
                    self.message_queue = self.message_queue[-max_queue_size:]
            
            logger.debug(f"[Node] 节点 {self.id} 收到消息，类型={msg_type}，队列大小={len(self.message_queue)}")
            
        except Exception as e:
            logger.error(f"[Node] 节点 {self.id} 入队消息异常: {e}")
    
    def get_queue_size(self) -> int:
        """获取当前队列大小"""
        with self.message_lock:
            return len(self.message_queue)

    def set_consensus(self, consensus_instance):
        """外部设置共识实例"""
        self.consensus = consensus_instance
        logger.info(f"节点 {self.id} 设置共识实例")

    def init_dependencies(self):
        """延迟初始化依赖（在所有组件注册完成后调用）"""
        self.consensus = self.services.get('consensus')
        self.mempool = self.services.get('mempool')
        self.network = self.services.get('network')

        logger.info(f"节点 {self.id} 初始化完成，索引={index}")
        
    def start_message_processing(self):
        """启动消息处理线程"""
        if not self.processing_active:
            self.processing_active = True
            self.processing_thread = threading.Thread(
                target=self._process_message_queue, 
                daemon=True,
                name=f"node_{self.id}_msg_processor"
            )
            self.processing_thread.start()
            logger.info(f"[Node] 启动消息处理线程: {self.id}")
    
    def stop_message_processing(self):
        """停止消息处理线程"""
        self.processing_active = False
        logger.info(f"[Node] 停止消息处理线程: {self.id}")
        
    # ==================== 消息处理（更新） ====================
    def receive_message(self, msg_type, payload):
        """
        异步处理消息（Network调用，消息入队）
        
        Args:
            msg_type: 消息类型
            payload: 消息内容
            
        Returns:
            bool: 是否成功入队
        """
        logger.info(f"[Node] 节点 {self.id} 收到消息，类型={msg_type}")
        
        # 将消息加入队列，由后台线程处理
        self.enqueue_message(msg_type, payload)
        return True

    # def receive_message(self, msg_type, payload):
    #     """
    #     同步处理消息（Network直接调用）
        
    #     Args:
    #         msg_type: 消息类型
    #         payload: 消息内容
            
    #     Returns:
    #         bool: 处理结果
    #     """
    #     logger.info(f"[Node] 节点 {self.id} 收到消息，类型={msg_type}")
        
    #     # 直接调用处理，不使用队列
    #     return self._process_single_message(msg_type, payload)
    
    # def receive_message(self, msg_type, payload):
    #     logger.info("处理所有类型的消息")
    #     # self.consensus.on_message(msg_type, payload)

    #     """处理所有类型的消息"""
    #     sender = payload.get("sender", "unknown")
        
    #     if msg_type == self.network.MSG_TYPES["PROPOSAL"]:
    #         return self.handle_proposal(self, payload)
    #     elif msg_type == self.network.MSG_TYPES["VOTE"]:
    #         return self.handle_vote(self, payload)
    #     elif msg_type == self.network.MSG_TYPES["NEW_VIEW"]:
    #         return self.handle_new_view(self, payload)
    #     elif msg_type == self.network.MSG_TYPES["TIMEOUT"]:
    #         return self.handle_timeout(self, payload)
    #     elif msg_type == self.network.MSG_TYPES["VIEW_CHANGE"]:
    #         return self.handle_view_change(self, payload)
    #     elif msg_type == self.network.MSG_TYPES["VIEW_CHANGE_COMPLETE"]:
    #         # 视图切换完成通知
    #         logger.info(f"[Node] 节点 {self.id} 收到视图切换完成通知")
    #         # 这里可以更新本地状态或确认同步
    #         return True
    #     else:
    #         logger.warning(f"[{self.id}] 未知消息类型: {msg_type}")
    
    # def handle_vote(self, vote_data: dict) -> bool:
    #     try:
    #         logger.info(f"节点 {self.id} 收到vote请求")
    #         if vote_data is None:
    #             logger.info(f"没有得到vote_data")
    #             return False

    #         result = self.consensus.handle_vote(self, vote_data)
            
    #         if result:
    #             logger.info(f"[Node] 节点 {self.id} 成功处理提案")
    #             # 更新统计信息
    #             if self.metrics:
    #                 # self.metrics.record_event("vote_data", self.id)
    #                 pass
    #         else:
    #             logger.warning(f"[Node] 节点 {self.id} 处理cote失败")
            
    #         return result
            
    #     except Exception as e:
    #         logger.error(f"[Node] 节点 {self.id} 处理vote异常: {e}", exc_info=True)
    #         return False

    def handle_vote(self, vote_data: dict) -> bool:
        """处理投票消息"""
        try:
            logger.info(f"[Node] 节点 {self.id} 收到vote请求，vote_data keys: {vote_data.keys()}")
            
            # 1. 从vote_data中提取信息
            sender_id = vote_data.get("sender")
            vote_dict = vote_data.get("vote")
            
            if not vote_dict:
                logger.error(f"[Node] vote_data中没有vote字段，vote_data: {vote_data}")
                return False
                
            if not sender_id:
                logger.error(f"[Node] vote_data中没有sender字段")
                return False
            
            # 2. 记录日志
            logger.info(f"[Node] 收到来自 {sender_id} 的投票，vote_dict类型: {type(vote_dict)}")
            
            # 3. 反序列化Vote对象
            vote = None
            if isinstance(vote_dict, Vote):
                # 如果已经是Vote对象，直接使用
                vote = vote_dict
                logger.info(f"[Node] vote_dict已经是Vote对象: {vote}")
            elif isinstance(vote_dict, dict):
                # 如果是字典，反序列化
                logger.info(f"[Node] 反序列化vote_dict: {vote_dict}")
                vote = Vote.from_dict(vote_dict) if hasattr(Vote, 'from_dict') else Vote(**vote_dict)
            else:
                logger.error(f"[Node] vote_dict是未知类型: {type(vote_dict)}")
                return False
            
            if not vote:
                logger.error(f"[Node] 无法创建vote对象")
                return False
            
            logger.info(f"[Node] 反序列化后的vote对象: {vote}, 类型: {type(vote)}")
            
            # 4. 记录投票到节点状态
            self.last_vote = {
                "vote": vote,
                "sender": sender_id,
                "timestamp": time.time()
            }
            
            # 5. 调用共识层处理
            if self.consensus:
                # 创建新的vote_data，包含反序列化后的vote对象
                processed_vote_data = {
                    "vote": vote,  # 传递Vote对象，而不是字典
                    "sender": sender_id,
                    "original_data": vote_data  # 保留原始数据用于调试
                }
                
                result = self.consensus.handle_vote(self, processed_vote_data)
                
                if result:
                    logger.info(f"[Node] 节点 {self.id} 成功处理投票")
                    if self.metrics:
                        pass
                        # self.metrics.record_event("vote_processed", self.id)
                else:
                    logger.warning(f"[Node] 节点 {self.id} 处理投票失败")
                
                return result
            else:
                logger.error(f"[Node] 共识实例未初始化")
                return False
                
        except Exception as e:
            logger.error(f"[Node] 节点 {self.id} 处理vote异常: {e}", exc_info=True)
            return False


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
                # 2025-12-16 21:45:57,683 - ERROR - 节点 node3 更新状态失败: 'NoneType' object has no attribute 'hash'
                logger.info(f"节点 {self.id} 更新QC: 视图 {old_view} -> {qc.view}")
            
            # 3. 更新本地视图号
            if qc and qc.view > self.view:
                old_view = self.view
                self.view = qc.view
                logger.info(f"节点 {self.id} 更新视图: {old_view} -> {self.view}")
            
            # 5. 清除当前提案（因为已经完成）
            if hasattr(self, 'current_proposal'):
                self.current_proposal = None
                logger.info(f"节点 {self.id} 清除当前提案")
                
            return True
            
        except Exception as e:
            logger.error(f"节点 {self.id} 更新状态失败: {e}")
            return False


    def handle_proposal(self, proposal_data: dict) -> bool:
        """
        Node层处理提案（调用Consensus层）
        
        Returns:
            bool: 是否接受提案
        """
        logger.info(f"[Node] 节点 {self.id} 收到提案消息")
        
        try:
            # 检查是否已经有当前视图的提案
            current_view = self.view
            if self.current_proposal and self.current_proposal.get("view") == current_view:
                logger.info(f"[Node] 节点 {self.id} 已有当前视图的提案，忽略重复提案")
                return False
            
            # 调用Consensus处理提案
            result = self.consensus.handle_proposal(self, proposal_data)
            
            if result:
                logger.info(f"[Node] 节点 {self.id} 成功处理提案")
                # 更新统计信息
                if self.metrics:
                    pass
                    # self.metrics.record_event("proposal_processed", self.id)
            else:
                logger.warning(f"[Node] 节点 {self.id} 处理提案失败")
            
            return result
            
        except Exception as e:
            logger.error(f"[Node] 节点 {self.id} 处理提案异常: {e}", exc_info=True)
            return False
    
    def handle_proof_request(self, request_data: dict) -> bool:
        """处理Merkle证明请求"""
        try:
            # 提取请求数据
            request_id = request_data.get("request_id")
            block_hash = request_data.get("block_hash")
            replica_index = request_data.get("replica_index")
            view = request_data.get("view")
            requester_id = request_data.get("sender")  # 请求者ID
            
            logger.info(f"[Node] 节点 {self.id} 收到Merkle证明请求: {request_id}")
            
            # 检查是否有所需的区块
            block = self.state.get_block_by_hash(block_hash)
            if not block:
                logger.warning(f"[Node] 节点 {self.id} 没有区块 {block_hash[:8].hex()}")
                return False
            
            # 生成Merkle证明（简化）
            # 实际应该根据QC和replica_index生成正确的证明
            merkle_proof = MerkleProof(
                replica_index=replica_index,
                view=view,
                block_hash=block_hash,
                partial_signature=self._get_partial_signature_for_block(block, view),
                sibling_hashes=[],  # 简化
                voter_id=self.id
            )
            
            # 发送证明回请求者
            self.network.send_merkle_proof(
                sender_id=self.id,
                request_id=request_id,
                merkle_proof=merkle_proof,
                target_id=requester_id
            )
            
            logger.info(f"[Node] 节点 {self.id} 发送Merkle证明给 {requester_id}")
            return True
            
        except Exception as e:
            logger.error(f"[Node] 处理Merkle证明请求失败: {e}")
            return False
    
    def handle_proof_response(self, response_data: dict) -> bool:
        """处理Merkle证明响应"""
        try:
            request_id = response_data.get("request_id")
            merkle_proof = response_data.get("merkle_proof")
            
            # 查找对应的请求（如果有的话）
            # 这里简化处理，直接缓存证明
            if merkle_proof:
                proof_key = (merkle_proof.view, merkle_proof.block_hash, merkle_proof.replica_index)
                self.cached_proofs[proof_key] = merkle_proof
                logger.info(f"[Node] 节点 {self.id} 缓存Merkle证明: {proof_key}")
                return True
            else:
                logger.warning(f"[Node] 收到空的Merkle证明")
                return False
                
        except Exception as e:
            logger.error(f"[Node] 处理Merkle证明响应失败: {e}")
            return False
    
    def _get_partial_signature_for_block(self, block, view: int) -> bytes:
        """获取对区块的部分签名"""
        try:
            # 准备签名数据
            sign_data = view.to_bytes(8, 'big') + block.hash
            
            # 使用BLS签名
            from crypto import BLS
            return BLS.sign(self.priv, sign_data)
        except Exception as e:
            logger.error(f"[Node] 生成部分签名失败: {e}")
            return b""  



    def handle_new_view(self, payload):
        """
        Node层处理NEW-VIEW消息
        调用Consensus层的实现
        """
        logger.info(f"[Node] 节点 {self.id} 处理NEW-VIEW消息")
        
        try:
            # 调用Consensus处理
            result = self.consensus.handle_new_view(self, payload)
            
            if result:
                logger.info(f"[Node] 节点 {self.id} 成功处理NEW-VIEW消息")
                if self.metrics:
                    pass
                    # self.metrics.record_event("new_view_processed", self.id)
            else:
                logger.warning(f"[Node] 节点 {self.id} 处理NEW-VIEW消息失败")
            
            return result
            
        except Exception as e:
            logger.error(f"[Node] 处理NEW-VIEW异常: {e}", exc_info=True)
            return False