import logging
import threading
import time
from utils412 import Block, QC, Mempool
from utils.logger import event


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

    def on_receive_pre_prepare(self, receiver_id, proposal_data):
        """Replica接收Leader广播的Pre-prepare消息（对应on_receive_proposal/handle_proposal）"""
        # 1. proposal_received埋点（已加）
        event(
            "proposal_received",
            node_id=receiver_id,
            view=proposal_data["view"],
            block_id=proposal_data["block"].block_id,
            consensus_type="PBFT"
        )

        # 2. 验证消息合法性
        if proposal_data["view"] != self.view:
            logging.warning(f"[Replica {receiver_id}] proposal view mismatch, ignore")
            return

        # 3. vote_sent埋点：Replica发送Prepare投票时触发
        block = proposal_data["block"]
        event(
            "vote_sent",
            node_id=receiver_id,  # 发送投票的Replica节点ID
            view=self.view,
            block_id=block.block_id,
            extra={"vote_type": "normal"},
            consensus_type="PBFT"
        )

        # 4. 生成Prepare投票（模拟Replica发送准备投票的逻辑）
        self.on_receive_vote("prepare", block.block_id, self.view, receiver_id)
        logging.info(f"[Replica {receiver_id}] received proposal {block.block_id}, sent prepare vote")

    def _detect_fork(self, block, leader_node):
        """分叉检测逻辑：检查新区块parent是否匹配节点最新区块，返回是否分叉+分叉链长度"""
        latest_block_id = leader_node.state.latest_qc.block_id if leader_node.state.latest_qc else None
        if block.parent_id is not None and latest_block_id is not None and block.parent_id != latest_block_id:
            # 简化：分叉链长度设为1（实际可根据链追溯计算）
            return True, 1
        return False, 0

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

            # 分叉检测 & fork_detected埋点
            is_fork, fork_length = self._detect_fork(block, leader)
            if is_fork:
                event(
                    "fork_detected",
                    node_id=leader_id,  # Leader检测到分叉（也可遍历所有节点触发）
                    view=self.view,
                    block_id=block.block_id,
                    extra={"fork_chain_length": fork_length},
                    consensus_type="PBFT"
                )
                logging.warning(f"[PBFT] fork detected at view {self.view}, block {block.block_id}")

            leader.state.add_block(block)

            # Proposal广播埋点（已加）
            event(
                "proposal_broadcast",
                node_id=leader_id,
                view=self.view,
                block_id=block.block_id,
                extra={"tx_count": len(block.payload)},
                consensus_type="PBFT"
            )

            # 广播预准备消息
            pre_prepare_data = {
                "block": block,
                "view": self.view,
                "round": r
            }
            self.network.broadcast(leader_id, "pre_prepare", pre_prepare_data)

            # 模拟网络层回调Replica的on_receive_pre_prepare
            for nid in self.nodes.keys():
                if nid != leader_id:
                    self.on_receive_pre_prepare(nid, pre_prepare_data)

            # 2. 准备阶段（Prepare）
            prepare_ok = self._collect_votes("prepare", block.block_id, self.view)
            if not prepare_ok:
                logging.warning(f"[PBFT] round {r} prepare stage failed, view change")
                # view_change埋点：prepare失败触发视图切换
                next_view = self.view + 1
                for nid in self.nodes.keys():
                    event(
                        "view_change",
                        node_id=nid,
                        view=next_view,
                        extra={"reason": "timeout"},
                        consensus_type="PBFT"
                    )
                self.view = next_view
                continue

            # 3. 提交阶段（Commit）
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
                    # block_committed埋点：每个节点确认区块提交时触发
                    event(
                        "block_committed",
                        node_id=nid,
                        view=self.view,
                        block_id=block.block_id
                    )
                logging.info(f"[PBFT] round {r} block {block.block_id} committed")
            else:
                logging.warning(f"[PBFT] round {r} commit stage failed, view change")
                # view_change埋点：commit失败触发视图切换
                next_view = self.view + 1
                for nid in self.nodes.keys():
                    event(
                        "view_change",
                        node_id=nid,
                        view=next_view,
                        extra={"reason": "timeout"},
                        consensus_type="PBFT"
                    )
                self.view = next_view
                continue

            # 正常视图切换（无失败）
            next_view = self.view + 1
            self.view = next_view
            time.sleep(self.proposal_interval + 0.2)

    def on_receive_vote(self, stage, block_id, view, voter_id):
        """节点收到投票后的处理（由网络层回调）"""
        with self.lock:
            key = (block_id, view)
            if key not in self.votes[stage]:
                self.votes[stage][key] = set()
            self.votes[stage][key].add(voter_id)