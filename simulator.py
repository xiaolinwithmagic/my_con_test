
import json
import time
import hashlib
import logging
import threading
import random
from did_sim import generate_multiple_identities
from asset_sim import generate_dummy_hashes
from transaction import create_register_tx, create_transfer_tx, create_license_tx, tx_id
from mempool import Mempool
from metrics import Metrics
from network import Network
from node import Node
from consensus import Consensus 
from block import Block
from transaction import verify_tx_signature

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

class Simulator:
    def __init__(self, num_nodes=4, f=1, txs_per_proposal=20, drop_rate=0.05):
        self.num_nodes = num_nodes
        self.f = f
        self.tx_per_prop = txs_per_proposal
        self.net = Network(drop_rate=drop_rate, delay_range=(0.01, 0.05))
        self.nodes = {}
        self.mempool = Mempool()
        self.metrics = Metrics()
        self.identities, self.did_pub = generate_multiple_identities(max(50, num_nodes + 10))  # generate some identities
        self.assets = generate_dummy_hashes(1000)

    def setup(self):
        node_ids = [f"node{i}" for i in range(self.num_nodes)]
        self.node_ids = node_ids
        # 1) create nodes with per-node identity
        for i, nid in enumerate(node_ids):
            ident = self.identities[i]
            node = Node(node_id=nid, priv_key=ident["priv"], pub_key=ident["pub"], network=self.net, f=self.f, all_nodes=node_ids)
            # keep DID->pub mapping as before
            node.did_pub_lookup = {x["did"]: x["pub"] for x in self.identities}
            node.metrics = self.metrics
            self.net.register(node)
            self.nodes[nid] = node

        # 2) after all nodes created, build node_id -> pub_key map and inject into each node
        nodeid_to_pub = { node_id: self.identities[i]["pub"] for i, node_id in enumerate(node_ids) }
        for nid, node in self.nodes.items():
            # add mapping from node id to pubkey so verify_aggregate can find pubkeys by signer (node id)
            node.did_pub_lookup.update(nodeid_to_pub)
            # optional: keep an explicit attribute
            node.node_pub_lookup = nodeid_to_pub

        # build consensus controller
        self.cons = Consensus(self.nodes, self.net, f=self.f)
        # attach mempool ref to consensus if necessary
        try:
            self.cons.mempool = self.mempool
        except Exception:
            pass

    def fill_mempool_registers(self, count=10000):
        creators = self.identities
        ncre = len(creators)
        for i in range(count):
            creator = creators[i % ncre]
            asset = self.assets[i % len(self.assets)]
            tx = create_register_tx(creator, asset)
            self.metrics.mark_created(tx_id(tx))
            self.mempool.push(tx)

    def run(self, rounds=100, attack=False):
        # start consensus background if needed (some Consensus.start returning thread)
        # We'll run a simple loop driving leader proposals (if Consensus doesn't already do it)
        t = threading.Thread(target=self._drive, args=(rounds, attack), daemon=True)
        t.start()
        return t

    def _drive(self, rounds, attack):
        for r in range(rounds):
            v = r + 1
            leader_id = self.cons.leader_for_view(v)
           # --------- FIX: 在广播 proposal 之前，显式同步每个 Node 的 view 与 is_leader ---------
            # leader_id = self.cons.leader_for_view(v)

            # 校验：Consensus 与 Node 的 all_nodes 映射是否一致（可选，但推荐）
            node_ids = list(self.nodes.keys())
            expected_leader = node_ids[v % len(node_ids)]
            if expected_leader != leader_id:
                logging.warning(f"[SIM] leader mismatch: consensus says {leader_id}, expected by node list {expected_leader} (view={v})")

            # 关键：为所有节点设置 view 和 is_leader（必须在广播前）
            for nid, node in self.nodes.items():
                node.view = v
                node.is_leader = (nid == leader_id)
                logging.debug(f"[SIM] set {nid}.view={node.view} is_leader={node.is_leader}")

            # 另外（可选），如果你希望触发 Node 内的 view-change 逻辑（例如 leader 收集 partial_qcs 等），可以调用：
            # for nid, node in self.nodes.items():
            #     node.handle_view_change(v)
            # 但注意 handle_view_change 会广播 view_change 消息到网络（可能引起额外日志）
            # ------------------------------------------------------------------------------------------------

            leader = self.nodes[leader_id]
            logging.info(f"[SIM] view={v} leader={leader_id} proposing")
            # collect txs
            txs = []
            for _ in range(self.tx_per_prop):
                tx = self.mempool.pop(timeout=0.05)
                if not tx:
                    break
                txs.append(tx)
            # malicious attack injection (split proposals) if requested
            if attack and r == max(1, rounds//3):
                # pick a registered asset to double-spend
                if txs:
                    base = txs[0]
                    img = base["image_hash"]
                    # pick alice as its creator (base["creator_did"])
                    alice = base["creator_did"]
                    # craft two conflicting transfers signed by alice's real priv
                    alice_priv = None
                    for idt in self.identities:
                        if idt["did"] == alice:
                            alice_priv = idt["priv"]
                            break
                    if alice_priv is None:
                        logging.warning("[SIM] can't find alice priv for attack; skipping")
                    else:
                        # build txA/tB and sign bytes manually
                        # import json, time, hashlib
                        # import json
                        # import time
                        # import hashlib
                        ta = {"type":"TRANSFER","image_hash":img,"from_did":alice,"to_did":"did:sim:attackerA","timestamp":int(time.time()*1000)}
                        tb = {"type":"TRANSFER","image_hash":img,"from_did":alice,"to_did":"did:sim:attackerB","timestamp":int(time.time()*1000)}
                        from did_sim import sign_message
                        ta["signature"] = sign_message(json.dumps({k:v for k,v in ta.items() if k!="signature"}, sort_keys=True, separators=(",",":")).encode(), alice_priv).hex()
                        tb["signature"] = sign_message(json.dumps({k:v for k,v in tb.items() if k!="signature"}, sort_keys=True, separators=(",",":")).encode(), alice_priv).hex()
                        # split nodes in two halves and send different proposal content
                        nodes_list = list(self.nodes.keys())
                        half = nodes_list[:len(nodes_list)//2]
                        other_half = nodes_list[len(nodes_list)//2:]
                        propA_block = Block(parent_id=getattr(leader.state.latest_qc,"block_id",None),
                                            height=r+1, proposer=leader_id, payload=None, qc=getattr(leader.state,"latest_qc",None))
                        propA = {"block":propA_block,"qc":getattr(leader.state,"latest_qc",None),"view":v,"transactions":[ta],"proposal_id":f"attackA-{v}"}
                        propB_block = Block(parent_id=getattr(leader.state.latest_qc,"block_id",None),
                                            height=r+1, proposer=leader_id, payload=None, qc=getattr(leader.state,"latest_qc",None))
                        propB = {"block":propB_block,"qc":getattr(leader.state,"latest_qc",None),"view":v,"transactions":[tb],"proposal_id":f"attackB-{v}"}
                        # deliver directly (bypass broadcast to simulate malicious leader)
                        for nid in half:
                            self.net._deliver(self.nodes[nid], "proposal", propA)
                        for nid in other_half:
                            self.net._deliver(self.nodes[nid], "proposal", propB)
                        logging.warning("[SIM] malicious leader injected split proposals")
                        # wait a bit
                        time.sleep(0.15)
                        continue

            # normal proposal: pack txs into a block and broadcast
            blk = Block(parent_id=getattr(leader.state.latest_qc,"block_id",None) if getattr(leader.state,"latest_qc",None) else None,
                        height=r+1, proposer=leader_id, payload=None, qc=getattr(leader.state,"latest_qc",None))
            blk.payload = {"transactions": txs}
            proposal = {"block": blk, "qc": getattr(leader.state,"latest_qc",None), "view": v, "transactions": txs, "proposal_id": f"p-{v}-{r}"}
            self.net.broadcast(leader_id, "proposal", proposal)
            # short sleep to simulate time between views
            time.sleep(0.15)
        logging.info("[SIM] finished driving rounds")
