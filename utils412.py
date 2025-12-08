import uuid
import logging
from collections import defaultdict

# 模拟Mempool
class Mempool:
    @staticmethod
    def get_txs(num):
        """生成模拟交易"""
        return [f"tx_{uuid.uuid4().hex[:8]}" for _ in range(num)]

# 模拟Block
class Block:
    def __init__(self, parent_id, height, proposer, payload, qc):
        self.block_id = uuid.uuid4().hex[:12]  # 唯一区块ID
        self.parent_id = parent_id
        self.height = height
        self.proposer = proposer
        self.payload = payload
        self.qc = qc

# 模拟QC（证书）
class QC:
    def __init__(self, block_id, view, votes):
        self.block_id = block_id
        self.view = view
        self.votes = votes  # 投票节点ID列表

# 模拟Node（节点）
class Node:
    def __init__(self, node_id):
        self.node_id = node_id
        self.is_leader = False
        # 节点状态
        self.state = self.State()

    class State:
        def __init__(self):
            self.latest_qc = None
            self.blocks = defaultdict(dict)  # 存储区块 {block_id: block}
            self.committed_blocks = set()  # 已提交区块ID

        def add_block(self, block):
            """添加区块到本地"""
            self.blocks[block.block_id] = block

        def commit_block(self, block_id):
            """提交区块"""
            self.committed_blocks.add(block_id)

# 模拟Network（网络层）
class Network:
    def __init__(self, consensus):
        self.consensus = consensus  # 关联的共识实例
        self.message_queue = defaultdict(list)  # 消息队列

    def broadcast(self, sender_id, msg_type, data):
        """模拟广播消息，直接触发投票收集（简化网络传输）"""
        logging.info(f"[NETWORK] {sender_id} broadcast {msg_type} for block {data.get('block').block_id if 'block' in data else data.get('block_id')}")
        # 模拟所有节点（除sender）回复投票
        for node_id in self.consensus.nodes.keys():
            if node_id == sender_id:
                continue
            # 模拟正常节点投赞成票
            if msg_type == "pre_prepare":
                self.consensus.on_receive_vote("prepare", data["block"].block_id, data["view"], node_id)
            elif msg_type == "prepare":
                self.consensus.on_receive_vote("commit", data["block_id"], data["view"], node_id)
            elif msg_type == "propose":
                self.consensus.on_receive_vote("prepare", data["block"].block_id, data["view"], node_id)
            elif msg_type == "pre_commit":
                self.consensus.on_receive_vote("pre_commit", data["block_id"], data["view"], node_id)
            elif msg_type == "commit":
                self.consensus.on_receive_vote("commit", data["block_id"], data["view"], node_id)