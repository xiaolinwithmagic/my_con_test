# state.py
import logging
from qc import QC

logging.basicConfig(level=logging.DEBUG)

class DummyQC:
    # def __init__(self):
    #     self.block_id = "genesis"
    #     self.view = 0
    #     self.agg = b""
    def __init__(self):
        self.block_id = "genesis"
        self.view = 0
        self.agg = b""  # 默认的空聚合签名
        self.signers = []  # 默认的空签名者列表

class NodeState:
    def __init__(self, f=1):
        self.locked_qc = DummyQC()
        self.latest_qc = DummyQC()
        self.commit_qc = DummyQC()
        self.voted_blocks = set()
        self.block_tree = {}
        self.f = f
        self.f_plus_1 = f + 1
        self.full_q_threshold = 2 * f + 1

    def update_locked_qc(self, qc: QC):
        if qc and qc.view > getattr(self.locked_qc, "view", 0):
            logging.debug(f"update_locked_qc -> {qc.block_id} (v{qc.view})")
            self.locked_qc = qc

    def update_latest_qc(self, qc: QC):
        if qc and qc.view > getattr(self.latest_qc, "view", 0):
            logging.debug(f"update_latest_qc -> {qc.block_id} (v{qc.view})")
            self.latest_qc = qc

    def add_block(self, block):
        if block.id not in self.block_tree:
            self.block_tree[block.id] = block

    def get_block(self, block_id):
        return self.block_tree.get(block_id)

    def has_voted(self, block_id):
        return block_id in self.voted_blocks

    def record_vote(self, block_id):
        logging.debug(f"record_vote {block_id}")
        self.voted_blocks.add(block_id)
