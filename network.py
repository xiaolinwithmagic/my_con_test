# network.py (修改后)
import random
import time
import logging
from threading import Thread
from typing import Any, Dict, Optional
from qc import QC, MerkleProof  # 导入新的数据结构

logging.basicConfig(level=logging.DEBUG)

class Network:
    def __init__(self, drop_rate: float = 0.1, delay_range: tuple = (0.05, 0.2)):
        self.nodes: Dict[str, Any] = {}  # node_id -> Node对象
        self.drop_rate = drop_rate
        self.delay_range = delay_range
        # self.services = services
        
        # 定义支持的消息类型常量
        self.MSG_TYPES = {
            "PROPOSAL": "proposal",
            "VOTE": "vote",
            "NEW_VIEW": "new_view",  # 携带新QC的消息
            "REQUEST_MERKLE_PROOF": "request_merkle_proof",
            "MERKLE_PROOF": "merkle_proof",
            "VIEW_CHANGE": "view_change",
            "TIMEOUT": "timeout",  # 超时消息，用于触发view change
        }


        
    def register(self, node) -> None:
        """注册节点到网络"""
        if node.id in self.nodes:
            logging.warning(f"Node {node.id} already registered.")
            return
        self.nodes[node.id] = node
        logging.debug(f"Node {node.id} registered to network (total: {len(self.nodes)})")
    
    def unregister(self, node_id: str) -> None:
        """从网络注销节点"""
        if node_id in self.nodes:
            del self.nodes[node_id]
            logging.debug(f"Node {node_id} unregistered from network")
    
    # def _should_drop(self) -> bool:
    #     """随机决定是否丢弃消息"""
    #     return random.random() < self.drop_rate
    
    def _get_delay(self) -> float:
        """获取随机延迟时间"""
        return random.uniform(*self.delay_range)
    
   
    def send_to(self, sender_id: str, receiver_id: str, message_type: str, message: Dict[str, Any]) -> None:
        """
        发送消息到特定节点（用于请求证明等点对点通信）
        """
        if receiver_id not in self.nodes:
            logging.error(f"Cannot send to {receiver_id}: node not registered")
            return
            
        if sender_id not in self.nodes:
            logging.error(f"Cannot send from {sender_id}: node not registered")
            return
            
        # 添加发送者信息到消息中
        message_with_sender = message.copy()
        message_with_sender["sender"] = sender_id
        message_with_sender["type"] = message_type
        
        receiver = self.nodes[receiver_id]
        
        # 使用新线程投递，避免阻塞
        Thread(target=self._deliver, 
               args=(receiver, message_type, message_with_sender),
               daemon=True).start()
    
    def broadcast(self, sender_id: str, message_type: str, message: Dict[str, Any]) -> int:
        """
        广播消息到所有其他节点
        """
        if sender_id not in self.nodes:
            logging.error(f"Cannot broadcast from {sender_id}: node not registered")
            return 0  # 返回0而不是None

        logging.debug(f"[Broadcast] {message_type} from {sender_id}")

        # 提前初始化变量
        threads = []
        total_targets = len(self.nodes) - 1  # 排除自己
        
        if total_targets <= 0:
            logging.debug(f"[Broadcast] 没有其他节点可发送")
            return 0
        
        # 步骤1: 为每个接收者创建独立的投递线程
        for node_id, node in self.nodes.items():
            if node_id == sender_id:
                continue  # 不发送给自己
                
            # 为每个接收者复制消息并添加发送者信息
            msg_copy = message.copy()
            msg_copy["sender"] = sender_id
            msg_copy["type"] = message_type
            
            # 创建线程
            thread = Thread(target=self._deliver, 
                        args=(node, message_type, msg_copy),
                        daemon=True)
            threads.append(thread)
        
        # 步骤2: 启动所有线程（在循环外部！）
        for thread in threads:
            thread.start()
        
        # 给线程一点时间开始执行
        time.sleep(0.05)
        
        # 步骤3: 等待所有线程完成（带超时）
        start_time = time.time()
        timeout = 5.0  # 5秒超时
        
        # 注意：这里我们使用一个新的变量名来避免混淆
        for current_thread in threads:
            remaining_time = timeout - (time.time() - start_time)
            if remaining_time > 0:
                current_thread.join(timeout=remaining_time)
            else:
                logging.warning(f"[Broadcast] 超时，部分线程未完成")
                break
        
        # 步骤4: 统计成功数量
        success_count = sum(1 for t in threads if not t.is_alive())
        logging.debug(f"[Broadcast] {message_type} from {sender_id} 成功发送给 {success_count}/{total_targets} 个节点")
        return success_count


    def broadcast_proposal(self, sender_id: str, block, qc: Optional[QC] = None, view: int = 0) -> None:
        """广播提案消息 - 修正版"""
        try:
            # 确保 block 被正确序列化
            if hasattr(block, "to_dict"):
                block_dict = block.to_dict()
            elif isinstance(block, dict):
                block_dict = block
            else:
                # 尝试将 block 转换为字典
                block_dict = block.__dict__
                logging.warning(f"Block converted to dict via __dict__: type={type(block)}")
            
            # 确保 QC 被正确序列化
            qc_dict = None
            if qc:
                if hasattr(qc, "to_dict"):
                    qc_dict = qc.to_dict()
                elif isinstance(qc, dict):
                    qc_dict = qc
                else:
                    qc_dict = qc.__dict__
                    logging.warning(f"QC converted to dict via __dict__: type={type(qc)}")
            
            message = {
                "block": block_dict,
                "qc": qc_dict,
                "view": view,
                "timestamp": time.time(),
            }
            
            # 添加调试信息
            logging.debug(f"[Broadcast] Proposal from {sender_id}, block hash: {block_dict.get('hash', 'unknown')[:8] if isinstance(block_dict, dict) else 'N/A'}")
            
            self.broadcast(sender_id, self.MSG_TYPES["PROPOSAL"], message)
        except Exception as e:
            logging.error(f"Error in broadcast_proposal: {e}")
    
    # def broadcast_vote(self, sender_id: str, block_id: str, view: int, 
    #               partial_sig: bytes, voter_index: int, block_hash: bytes = None) -> bool:
    #     """广播投票消息"""
    #     message = {
    #         "voter_id": sender_id,  # 发送者ID
    #         "voter_index": voter_index,  # 发送者索引
    #         "block_id": block_id,  # 区块ID
    #         "block_hash": block_hash.hex() if block_hash else None,  # 区块哈希
    #         "view": view,  # 视图号
    #         "partial_signature": partial_sig.hex() if partial_sig else None,  # 部分签名
    #         "timestamp": time.time(),
    #     }
    #     self.broadcast(sender_id, self.MSG_TYPES["VOTE"], message)
    #     logging.debug(f"[Broadcast] Vote from {sender_id} for block {block_id[:8] if block_id else 'unknown'}")
    #     return True

    def broadcast_vote(self, sender_id: str, vote: 'Vote', block: 'Block') -> bool:
        """广播投票消息（使用Vote对象）"""
        try:
            if vote is None:
                logging.error(f"[Network] 投票对象为空，无法广播")
                return False
                
            if block is None:
                logging.error(f"[Network] 区块对象为空，无法广播")
                return False
            
            # 使用Vote对象的to_dict方法序列化
            vote_dict = vote.to_dict()
            
            # 添加额外信息
            message = {
                "vote": vote_dict,  # 序列化后的投票
                "block": {
                    "id": block.id,
                    "hash": block.hash,
                    "height": block.height if hasattr(block, 'height') else None,
                    "proposer": block.proposer if hasattr(block, 'proposer') else None,
                    "transactions": block.transactions if hasattr(block, 'transactions') else [],
                    "timestamp": block.timestamp if hasattr(block, 'timestamp') else time.time()
                },
                "timestamp": time.time()
            }
            
            logging.info(f"[Network] 节点 {sender_id} 广播投票，view={vote.view}, block={block.id[:8]}")
            
            # 广播消息
            success_count = self.broadcast(sender_id, self.MSG_TYPES["VOTE"], message)
            logging.info(f"[Network] 广播投票完成")
            
            # 判断是否成功（至少发送给大多数节点）
            # total_nodes = len(self.nodes) - 1  # 排除自己
            # if total_nodes > 0:
            #     success_rate = success_count / total_nodes
            #     success = success_rate >= 0  # 至少50%成功
            #     logging.info(f"[Network] 投票广播结果: {success_count}/{total_nodes} ({success_rate:.1%}), 成功: {success}")
            #     return success
            # else:
            #     logging.info(f"[Network] 没有其他节点可发送")
            #     return True  # 没有其他节点，视为成功
            return True
                
        except Exception as e:
            logging.error(f"[Network] 广播投票异常: {e}", exc_info=True)
            return False
    
    def broadcast_new_view(self, sender_id: str, qc: QC) -> None:
        """广播新视图消息（携带新的QC）"""
        message = {
            "qc": qc.to_dict(),
            "view": qc.view,
            "timestamp": time.time(),
        }
        self.broadcast(sender_id, self.MSG_TYPES["NEW_VIEW"], message)
    
    def request_merkle_proof(self, requester_id: str, leader_id: str, 
                           replica_index: int, view: int, block_hash: bytes) -> None:
        """
        请求Merkle证明（副本 -> Leader）
        """
        message = {
            "replica_index": replica_index,
            "view": view,
            "block_hash": block_hash.hex() if block_hash else None,
            "timestamp": time.time(),
        }
        self.send_to(requester_id, leader_id, 
                    self.MSG_TYPES["REQUEST_MERKLE_PROOF"], message)
        
        logging.debug(f"[Proof Request] {requester_id} -> {leader_id} " 
                     f"(index={replica_index}, view={view})")
    
    def send_merkle_proof(self, sender_id: str, receiver_id: str, 
                        proof: MerkleProof) -> None:
        """
        发送Merkle证明（Leader -> 副本）
        """
        message = {
            "proof": proof.to_dict(),
            "timestamp": time.time(),
        }
        self.send_to(sender_id, receiver_id, 
                    self.MSG_TYPES["MERKLE_PROOF"], message)
        
        logging.debug(f"[Proof Response] {sender_id} -> {receiver_id} "
                     f"(index={proof.replica_index}, view={proof.view})")
    
    def broadcast_view_change(self, sender_id: str, new_view: int, 
                            reason: str = "", high_qc: Optional[QC] = None) -> None:
        """广播视图切换消息"""
        message = {
            "new_view": new_view,
            "reason": reason,
            "high_qc": high_qc.to_dict() if high_qc else None,
            "timestamp": time.time(),
        }
        self.broadcast(sender_id, self.MSG_TYPES["VIEW_CHANGE"], message)
        logging.info(f"[View Change] {sender_id} initiates view change to {new_view}: {reason}")
    
    def broadcast_timeout(self, sender_id: str, current_view: int) -> None:
        """广播超时消息（用于触发view change）"""
        message = {
            "current_view": current_view,
            "timestamp": time.time(),
        }
        self.broadcast(sender_id, self.MSG_TYPES["TIMEOUT"], message)
        logging.debug(f"[Timeout] {sender_id} times out at view {current_view}")
    
    # ------------------------------------------------------------
    # 旧方法的兼容性包装（如果需要）
    # ------------------------------------------------------------
    
    def broadcast_soft_vote(self, sender_id: str, block_id: str):
        """兼容旧方法 - 软投票"""
        logging.warning("broadcast_soft_vote is deprecated, use broadcast_vote instead")
        self.broadcast(sender_id, "soft_vote", {"block_id": block_id, "sender": sender_id})
    
    def broadcast_partial_signature(self, sender_id: str, partial_sig):
        """兼容旧方法 - 部分签名"""
        logging.warning("broadcast_partial_signature is deprecated, use broadcast_vote instead")
        self.broadcast(sender_id, "partial_signature", partial_sig)
    
    def broadcast_partial_qc(self, sender_id: str, partial_certificate):
        """兼容旧方法 - 部分QC"""
        logging.warning("broadcast_partial_qc is deprecated, use broadcast_new_view instead")
        self.broadcast(sender_id, "partial_qc", partial_certificate)
    
    # ------------------------------------------------------------
    # 辅助方法
    # ------------------------------------------------------------
    
    def get_node_ids(self) -> list:
        """获取所有已注册节点的ID"""
        return list(self.nodes.keys())
    
    def get_node_count(self) -> int:
        """获取网络中的节点数量"""
        return len(self.nodes)
    
    def is_registered(self, node_id: str) -> bool:
        """检查节点是否已注册"""
        return node_id in self.nodes
    
    def clear(self) -> None:
        """清空网络中的所有节点（用于测试）"""
        self.nodes.clear()
        logging.debug("Network cleared")



    def _deliver(self, receiver, message_type: str, message: Dict[str, Any]) -> None:
        """
        内部投递方法：模拟网络延迟和丢包
        """
        # 检查消息是否被丢弃
        # if self._should_drop():
        #     logging.warning(f"✗ Message {message_type} to {receiver.id} dropped.")
        #     return
            
        # 模拟网络延迟
        delay = self._get_delay()
        if delay > 0:
            time.sleep(delay)
            
        # 记录投递日志
        src = message.get("sender", "unknown")
        logging.debug(f"✓ {message_type} from {src} to {receiver.id} (delay: {delay:.3f}s)")
        
        # 投递消息到接收者
        try:
            # 添加调试信息
            if message_type == self.MSG_TYPES["PROPOSAL"]:
                logging.debug(f"[DEBUG] Delivering Proposal to {receiver.id}")
            #     logging.debug(f"[DEBUG] Proposal block type: {type(message.get('block'))}")
            #     logging.debug(f"[DEBUG] Proposal block keys: {list(message.get('block', {}).keys())[:3]}...")
            
            receiver.receive_message(message_type, message)
        except Exception as e:
            logging.error(f"Failed to deliver {message_type} to {receiver.id}: {e}")
            import traceback
            traceback.print_exc()

    def broadcast_qc_acceptance(self, sender_id: str, qc_view: int, block_id: str, 
                               accept_count: int, timestamp: float):
        """广播QC接受通知（可选）"""
        message = {
            "type": "QC_ACCEPTANCE_NOTIFICATION",
            "sender": sender_id,
            "qc_view": qc_view,
            "block_id": block_id,
            "accept_count": accept_count,
            "timestamp": timestamp
        }
        
        for node in self.nodes.values():
            if node.id != sender_id:
                node.receive_message(self.MSG_TYPES.get("QC_ACCEPTANCE_NOTIFICATION", 999), message)

