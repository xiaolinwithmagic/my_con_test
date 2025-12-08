# votes.py
class Vote:
    def __init__(self, voter_id, block_id, signature, view):
        self.voter_id = voter_id
        self.block_id = block_id
        self.signature = signature
        self.view = view

    def to_dict(self):
        return {
            "voter_id": self.voter_id,
            "block_id": self.block_id,
            "signature": self.signature,
            "view": self.view
        }

def is_conflicting_qc(qc1, qc2):
    if not qc1 or not qc2:
        return False
    return qc1.block_id != qc2.block_id and qc1.view == qc2.view
