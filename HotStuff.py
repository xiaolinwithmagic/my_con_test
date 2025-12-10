import logging
import threading
import time
from utils412 import Block, QC, Mempool
from utils.logger import event  # 新增导入event

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

    def _detect_fork(self, block, leader_node):
        """分叉检测：检查新区块parent是否匹配high_qc/最新区块，返回是否分叉+链长度"""
        latest_block_id = self.high_qc.block_id if self.high_qc else None
        if block.parent_id is not None and latest_block_id is not None and block.parent_id != latest_block_id:
            # 简化：分叉链长度为当前区块高度 - 父区块高度（实际可追溯链）
            fork_length = block.height - (leader_node.state.get_block(block.parent_id).height if leader_node.state.get_block(block.parent_id) else 0)
            return True, fork_length
        return False, 0

    def on_receive_proposal(self, receiver_id, proposal_data):
        """Replica接收提案消息的处理（对应on_receive_proposal/handle_proposal）"""
        # 1. proposal_received埋点
        event(
            "proposal_received",
            node_id=receiver_id,
            view=proposal_data["view"],
            block_id=proposal_data["block"].block_id,
            consensus_type="htf"
        )

        # 2. 验证视图合法性
        if proposal_data["view"] != self.view:
            logging.warning(f"[HotStuff Replica {receiver_id}] proposal view mismatch, ignore")
            return

        # 3. vote_sent埋点：发送Prepare投票前触发
        block = proposal_data["block"]
        event(
            "vote_sent",
            node_id=receiver_id,
            view=self.view,
            block_id=block.block_id,
            extra={"vote_type": "normal"},
            consensus_type="htf"
        )

        # 4. 模拟Replica发送Prepare投票（网络层回调on_receive_vote）
        self.on_receive_vote("prepare", block.block_id, self.view, receiver_id)
        logging.info(f"[HotStuff Replica {receiver_id}] sent prepare vote for block {block.block_id}")

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

            # 分叉检测 & fork_detected埋点
            is_fork, fork_length = self._detect_fork(block, leader)
            if is_fork:
                event(
                    "fork_detected",
                    node_id=leader_id,
                    view=self.view,
                    block_id=block.block_id,
                    extra={"fork_chain_length": fork_length},
                    consensus_type="htf"
                )
                logging.warning(f"[HotStuff] fork detected at view {self.view}, block {block.block_id}")

            leader.state.add_block(block)

            # proposal_broadcast埋点：Leader广播提案前触发
            event(
                "proposal_broadcast",
                node_id=leader_id,
                view=self.view,
                block_id=block.block_id,
                extra={"tx_count": len(block.payload)},
                consensus_type="htf"
            )

            # 广播提案
            proposal_data = {
                "block": block,
                "view": self.view
            }
            self.network.broadcast(leader_id, "propose", proposal_data)

            # 模拟Replica接收提案（实际由网络层异步回调）
            for nid in self.nodes.keys():
                if nid != leader_id:
                    self.on_receive_proposal(nid, proposal_data)

            # 2. 准备阶段（Prepare）：收集准备投票，生成prepare QC
            prepare_ok = self._collect_votes("prepare", block.block_id, self.view)
            if not prepare_ok:
                logging.warning(f"[HotStuff] round {r} prepare failed, view change")
                # view_change埋点：prepare失败触发视图切换
                next_view = self.view + 1
                for nid in self.nodes.keys():
                    event(
                        "view_change",
                        node_id=nid,
                        view=next_view,
                        extra={"reason": "timeout"},
                        consensus_type="htf"
                    )
                self.view = next_view
                continue

            # qc_built埋点：生成prepare QC后触发
            prepare_qc = QC(block_id=block.block_id, view=self.view, votes=list(self.votes["prepare"][(block.block_id, self.view)]))
            event(
                "qc_built",
                node_id=leader_id,
                view=prepare_qc.view,
                block_id=prepare_qc.block_id,
                extra={"signer_count": len(prepare_qc.votes)} ,
                consensus_type="htf"
            )

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
                # view_change埋点：pre_commit失败触发视图切换
                next_view = self.view + 1
                for nid in self.nodes.keys():
                    event(
                        "view_change",
                        node_id=nid,
                        view=next_view,
                        extra={"reason": "timeout"},
                        consensus_type="htf"
                    )
                self.view = next_view
                continue

            # qc_built埋点：生成pre_commit QC后触发
            pre_commit_qc = QC(block_id=block.block_id, view=self.view, votes=list(self.votes["pre_commit"][(block.block_id, self.view)]))
            event(
                "qc_built",
                node_id=leader_id,
                view=pre_commit_qc.view,
                block_id=pre_commit_qc.block_id,
                extra={"signer_count": len(pre_commit_qc.votes)},
                consensus_type="htf"
            )

            # 4. 提交阶段（Commit）：更新high_qc，确认区块提交
            self.network.broadcast(leader_id, "commit", {
                "block_id": block.block_id,
                "qc": pre_commit_qc,
                "view": self.view
            })
            commit_ok = self._collect_votes("commit", block.block_id, self.view)
            if commit_ok:
                # qc_built埋点：生成commit阶段的high_qc后触发
                self.high_qc = QC(block_id=block.block_id, view=self.view, votes=list(self.votes["commit"][(block.block_id, self.view)]))
                event(
                    "qc_built",
                    node_id=leader_id,
                    view=self.high_qc.view,
                    block_id=self.high_qc.block_id,
                    extra={"signer_count": len(self.high_qc.votes)},
                    consensus_type="htf"
                )

                # 所有节点确认区块提交 & block_committed埋点
                for nid, node in self.nodes.items():
                    node.state.latest_qc = self.high_qc
                    node.state.commit_block(block.block_id)
                    event(
                        "block_committed",
                        node_id=nid,
                        view=self.view,
                        block_id=block.block_id,
                        consensus_type="htf"
                    )
                logging.info(f"[HotStuff] round {r} block {block.block_id} committed")
            else:
                logging.warning(f"[HotStuff] round {r} commit failed, view change")
                # view_change埋点：commit失败触发视图切换
                next_view = self.view + 1
                for nid in self.nodes.keys():
                    event(
                        "view_change",
                        node_id=nid,
                        view=next_view,
                        extra={"reason": "timeout"},
                        consensus_type="htf"
                    )
                self.view = next_view
                continue

            self.view += 1
            time.sleep(self.proposal_interval + 0.2)

    def on_receive_vote(self, stage, block_id, view, voter_id):
        """接收节点投票（网络层回调）"""
        with self.lock:
            key = (block_id, view)
            if key not in self.votes[stage]:
                self.votes[stage][key] = set()
            self.votes[stage][key].add(voter_id)