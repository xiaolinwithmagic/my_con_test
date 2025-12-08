# block.py
import hashlib
import json
import time

class Block:
    def __init__(self, parent_id, height, proposer, payload, qc=None, view=0):
        self.parent_id = parent_id
        self.height = height
        self.proposer = proposer
        self.payload = payload
        self.qc = qc  # can be QC object or None
        self.timestamp = time.time()
        self.view = view
        # id uses stable serialization; ensure qc is represented by qc.block_id or None
        self.id = self.calculate_hash()

    def calculate_hash(self):
        block_data = {
            "parent_id": self.parent_id,
            "height": self.height,
            "proposer": self.proposer,
            "payload": self.payload,
            "qc_block": getattr(self.qc, "block_id", None),
            "qc_view": getattr(self.qc, "view", None),
            "timestamp": self.timestamp,
            "view": self.view,
        }
        block_string = json.dumps(block_data, sort_keys=True)
        return hashlib.sha256(block_string.encode()).hexdigest()

    def validate(self):
        if self.height < 0:
            return False
        if not self.proposer:
            return False
        return True

    def to_dict(self):
        return {
            "id": self.id,
            "parent_id": self.parent_id,
            "height": self.height,
            "proposer": self.proposer,
            "payload": self.payload,
            "qc_block": getattr(self.qc, "block_id", None),
            "qc_view": getattr(self.qc, "view", None),
            "timestamp": self.timestamp,
            "view": self.view,
        }

    @staticmethod
    def from_dict(d):
        b = Block(d["parent_id"], d["height"], d["proposer"], d["payload"], qc=None, view=d.get("view", 0))
        b.timestamp = d["timestamp"]
        b.id = d["id"]
        return b

    def __repr__(self):
        return f"Block(id={self.id[:8]}, h={self.height}, proposer={self.proposer})"
