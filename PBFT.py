import logging
import threading
import time
from utils412 import Block, QC, Mempool

logging.basicConfig(level=logging.INFO)
TXS_PER_PROPOSAL = 20

class PBFTConsensus:
    def __init__(self, nodes, network, f=1):
        self.nodes = nodes  # 节点字典 {node_id: node实例}
        self.network = network  # 网络层，提供broadcast/recv接口
        self.f = f  # 容错数，最多容忍f个拜占庭节点
        self.n = len(nodes)  # 总节点数
        self.view = 1  # 当前视图
        self.proposal_interval = 1.0  # 提案间隔
        self.votes = {  # 存储各阶段投票
            "prepare": {},
            "commit": {}
        }
        self.lock = threading.Lock()  # 线程安全锁

    def leader_for_view(self, view):
        """视图对应的leader节点ID"""
        ids = sorted(self.nodes.keys())
        return ids[view % len(ids)]

    def start(self, rounds=5):
        """启动共识线程"""
        t = threading.Thread(target=self._run, args=(rounds,), daemon=True)
        t.start()
        return t

    def _collect_votes(self, stage, block_id, view, timeout=1.0):
        """收集指定阶段的投票，返回是否达到2f+1多数"""
        start = time.time()
        while time.time() - start < timeout:
            with self.lock:
                votes = self.votes[stage].get((block_id, view), set())
                if len(votes) >= 2 * self.f + 1:
                    return True
            time.sleep(0.1)
        return False

    def _reset_votes(self, stage=None):
        """重置投票记录"""
        with self.lock:
            if stage:
                self.votes[stage] = {}
            else:
                self.votes = {"prepare": {}, "commit": {}}

    def _run(self, rounds):
        """PBFT核心共识流程：预准备→准备→提交"""
        for r in range(rounds):
            self._reset_votes()
            leader_id = self.leader_for_view(self.view)
            leader = self.nodes[leader_id]
            logging.info(f"[PBFT] view={self.view} leader={leader_id} start round {r}")

            # 1. 预准备阶段（Pre-prepare）
            parent = leader.state.latest_qc.block_id if leader.state.latest_qc else None
            txs = Mempool.get_txs(TXS_PER_PROPOSAL)
            block = Block(
                parent_id=parent,
                height=r + 1,
                proposer=leader_id,
                payload=txs,
                qc=leader.state.latest_qc
            )
            leader.state.add_block(block)
            # 广播预准备消息
            self.network.broadcast(leader_id, "pre_prepare", {
                "block": block,
                "view": self.view,
                "round": r
            })

            # 2. 准备阶段（Prepare）
            # 非leader节点收到预准备后会发送准备投票，此处简化为模拟收集投票
            prepare_ok = self._collect_votes("prepare", block.block_id, self.view)
            if not prepare_ok:
                logging.warning(f"[PBFT] round {r} prepare stage failed, view change")
                self.view += 1
                continue

            # 3. 提交阶段（Commit）
            # 广播提交消息，收集提交投票
            self.network.broadcast(leader_id, "commit", {
                "block_id": block.block_id,
                "view": self.view
            })
            commit_ok = self._collect_votes("commit", block.block_id, self.view)
            if commit_ok:
                # 生成QC证书，确认区块提交
                qc = QC(block_id=block.block_id, view=self.view, votes=list(self.votes["commit"][(block.block_id, self.view)]))
                for nid, node in self.nodes.items():
                    node.state.latest_qc = qc
                logging.info(f"[PBFT] round {r} block {block.block_id} committed")
            else:
                logging.warning(f"[PBFT] round {r} commit stage failed, view change")

            self.view += 1
            time.sleep(self.proposal_interval + 0.2)

    def on_receive_vote(self, stage, block_id, view, voter_id):
        """节点收到投票后的处理（由网络层回调）"""
        with self.lock:
            key = (block_id, view)
            if key not in self.votes[stage]:
                self.votes[stage][key] = set()
            self.votes[stage][key].add(voter_id)