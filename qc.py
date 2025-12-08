# qc.py
from dataclasses import dataclass

@dataclass
class QC:
    block_id: str
    signatures: list  # list of partial signatures
    signers: list     # list of signer ids
    view: int
    agg: bytes = None

    def to_dict(self):
        return {
            "block_id": self.block_id,
            "signers": self.signers,
            "view": self.view,
            "agg": self.agg
        }

@dataclass
class PartialQC:
    block_id: str
    view: int
    aggregated_sig: bytes
    signer_count: int

    def to_dict(self):
        return {
            "block_id": self.block_id,
            "view": self.view,
            "aggregated_sig": self.aggregated_sig,
            "signer_count": self.signer_count
        }
