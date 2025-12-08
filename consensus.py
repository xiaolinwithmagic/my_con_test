# consensus.py
import logging
import threading
import time
from block import Block
from qc import QC
from mempool import Mempool

logging.basicConfig(level=logging.INFO)

TXS_PER_PROPOSAL = 20

class Consensus:
    def __init__(self, nodes, network, f=1):
        self.nodes = nodes
        self.network = network
        self.f = f
        self.n = len(nodes)
        self.view = 1
        self.proposal_interval = 1.0

    def leader_for_view(self, view):
        ids = sorted(self.nodes.keys())
        return ids[view % len(ids)]

    def start(self, rounds=5):
        t = threading.Thread(target=self._run, args=(rounds,), daemon=True)
        t.start()
        return t

    def _run(self, rounds):
        for r in range(rounds):
            leader_id = self.leader_for_view(self.view)
            leader = self.nodes[leader_id]
            logging.info(f"[CONSENSUS] view={self.view} leader={leader_id} proposing round {r}")

            for nid, n in self.nodes.items():
                n.is_leader = (nid == leader_id)

            parent = leader.state.latest_qc.block_id if leader.state.latest_qc else None

            # --- 取交易（关键） ---
            txs = Mempool.get_txs(TXS_PER_PROPOSAL)

            block = Block(
                parent_id=parent,
                height=r + 1,
                proposer=leader_id,
                payload=txs,
                qc=leader.state.latest_qc
            )

            leader.state.add_block(block)

            self.network.broadcast(leader_id, "proposal",
                {"block": block, "qc": leader.state.latest_qc, "view": self.view})

            time.sleep(self.proposal_interval)

            self.view += 1
            time.sleep(0.2)
