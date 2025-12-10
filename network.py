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
    
    def _should_drop(self) -> bool:
        """随机决定是否丢弃消息"""
        return random.random() < self.drop_rate
    
    def _get_delay(self) -> float:
        """获取随机延迟时间"""
        return random.uniform(*self.delay_range)
    
    def _deliver(self, receiver, message_type: str, message: Dict[str, Any]) -> None:
        """
        内部投递方法：模拟网络延迟和丢包
        """
        # 检查消息是否被丢弃
        if self._should_drop():
            logging.warning(f"✗ Message {message_type} to {receiver.id} dropped.")
            return
            
        # 模拟网络延迟
        delay = self._get_delay()
        if delay > 0:
            time.sleep(delay)
            
        # 记录投递日志
        src = message.get("sender", "unknown")
        logging.debug(f"✓ {message_type} from {src} to {receiver.id} (delay: {delay:.3f}s)")
        
        # 投递消息到接收者
        try:
            receiver.receive_message(message_type, message)
        except Exception as e:
            logging.error(f"Failed to deliver {message_type} to {receiver.id}: {e}")
    
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
    
    def broadcast(self, sender_id: str, message_type: str, message: Dict[str, Any]) -> None:
        """
        广播消息到所有其他节点
        """
        if sender_id not in self.nodes:
            logging.error(f"Cannot broadcast from {sender_id}: node not registered")
            return
            
        logging.debug(f"[Broadcast] {message_type} from {sender_id}")
        
        # 为每个接收者创建独立的投递线程
        for node_id, node in self.nodes.items():
            if node_id == sender_id:
                continue  # 不发送给自己
                
            # 为每个接收者复制消息并添加发送者信息
            msg_copy = message.copy()
            msg_copy["sender"] = sender_id
            msg_copy["type"] = message_type
            
            Thread(target=self._deliver, 
                   args=(node, message_type, msg_copy),
                   daemon=True).start()
    
    # ------------------------------------------------------------
    # 具体的消息广播方法（针对不同的消息类型）
    # ------------------------------------------------------------
    
    def broadcast_proposal(self, sender_id: str, block, qc: Optional[QC] = None, view: int = 0) -> None:
        """广播提案消息"""
        message = {
            "block": block.to_dict() if hasattr(block, "to_dict") else block,
            "qc": qc.to_dict() if qc else None,
            "view": view,
            "timestamp": time.time(),
        }
        self.broadcast(sender_id, self.MSG_TYPES["PROPOSAL"], message)
    
    def broadcast_vote(self, sender_id: str, block_id: str, view: int, 
                      partial_sig: bytes, voter_index: int) -> None:
        """广播投票消息"""
        message = {
            "block_id": block_id,
            "block_hash": None,  # 将由接收者补充
            "view": view,
            "partial_sig": partial_sig.hex() if partial_sig else None,
            "voter_index": voter_index,
            "timestamp": time.time(),
        }
        self.broadcast(sender_id, self.MSG_TYPES["VOTE"], message)
    
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