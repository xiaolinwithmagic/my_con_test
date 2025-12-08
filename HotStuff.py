import logging
import threading
import time
from utils412 import Block, QC, Mempool

logging.basicConfig(level=logging.INFO)
TXS_PER_PROPOSAL = 20

class HotStuffConsensus:
    def __init__(self, nodes, network, f=1):
        self.nodes = nodes
        self.network = network
        self.f = f
        self.n = len(nodes)
        self.view = 1
        self.proposal_interval = 1.0
        self.locked_block = None  # HotStuff锁定的区块
        self.high_qc = None  # 最高QC证书
        self.votes = {  # 各阶段投票
            "prepare": {},
            "pre_commit": {},
            "commit": {}
        }
        self.lock = threading.Lock()

    def leader_for_view(self, view):
        ids = sorted(self.nodes.keys())
        return ids[view % len(ids)]

    def start(self, rounds=5):
        t = threading.Thread(target=self._run, args=(rounds,), daemon=True)
        t.start()
        return t

    def _collect_votes(self, stage, block_id, view, timeout=1.0):
        """收集投票，需达到2f+1多数"""
        start = time.time()
        while time.time() - start < timeout:
            with self.lock:
                votes = self.votes[stage].get((block_id, view), set())
                if len(votes) >= 2 * self.f + 1:
                    return True
            time.sleep(0.1)
        return False

    def _reset_votes(self):
        with self.lock:
            self.votes = {"prepare": {}, "pre_commit": {}, "commit": {}}

    def _run(self, rounds):
        """HotStuff核心流程：提案→准备→预提交→提交"""
        for r in range(rounds):
            self._reset_votes()
            leader_id = self.leader_for_view(self.view)
            leader = self.nodes[leader_id]
            logging.info(f"[HotStuff] view={self.view} leader={leader_id} start round {r}")

            # 1. 提案阶段（Propose）
            parent = self.high_qc.block_id if self.high_qc else None
            txs = Mempool.get_txs(TXS_PER_PROPOSAL)
            block = Block(
                parent_id=parent,
                height=r + 1,
                proposer=leader_id,
                payload=txs,
                qc=self.high_qc
            )
            leader.state.add_block(block)
            # 广播提案
            self.network.broadcast(leader_id, "propose", {
                "block": block,
                "view": self.view
            })

            # 2. 准备阶段（Prepare）：收集准备投票，生成prepare QC
            prepare_ok = self._collect_votes("prepare", block.block_id, self.view)
            if not prepare_ok:
                logging.warning(f"[HotStuff] round {r} prepare failed, view change")
                self.view += 1
                continue
            prepare_qc = QC(block_id=block.block_id, view=self.view, votes=list(self.votes["prepare"][(block.block_id, self.view)]))

            # 3. 预提交阶段（Pre-commit）：锁定区块，收集预提交投票
            self.locked_block = block
            self.network.broadcast(leader_id, "pre_commit", {
                "block_id": block.block_id,
                "qc": prepare_qc,
                "view": self.view
            })
            pre_commit_ok = self._collect_votes("pre_commit", block.block_id, self.view)
            if not pre_commit_ok:
                logging.warning(f"[HotStuff] round {r} pre-commit failed, view change")
                self.view += 1
                continue
            pre_commit_qc = QC(block_id=block.block_id, view=self.view, votes=list(self.votes["pre_commit"][(block.block_id, self.view)]))

            # 4. 提交阶段（Commit）：更新high_qc，确认区块提交
            self.network.broadcast(leader_id, "commit", {
                "block_id": block.block_id,
                "qc": pre_commit_qc,
                "view": self.view
            })
            commit_ok = self._collect_votes("commit", block.block_id, self.view)
            if commit_ok:
                self.high_qc = QC(block_id=block.block_id, view=self.view, votes=list(self.votes["commit"][(block.block_id, self.view)]))
                # 所有节点确认区块提交
                for nid, node in self.nodes.items():
                    node.state.latest_qc = self.high_qc
                    node.state.commit_block(block.block_id)
                logging.info(f"[HotStuff] round {r} block {block.block_id} committed")
            else:
                logging.warning(f"[HotStuff] round {r} commit failed, view change")

            self.view += 1
            time.sleep(self.proposal_interval + 0.2)

    def on_receive_vote(self, stage, block_id, view, voter_id):
        """接收节点投票（网络层回调）"""
        with self.lock:
            key = (block_id, view)
            if key not in self.votes[stage]:
                self.votes[stage][key] = set()
            self.votes[stage][key].add(voter_id)