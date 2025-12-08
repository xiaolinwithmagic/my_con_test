# node.py
import logging
import threading
import hashlib
import json
from state import NodeState, DummyQC
from votes import Vote, is_conflicting_qc
from qc import QC, PartialQC
from crypto import BLS, sign, aggregate, verify_aggregate
from world_state import WorldState
from metrics import Metrics

logging.basicConfig(level=logging.DEBUG)

SOFT_VOTE_TIMEOUT = 0.6
DEFER_TIMEOUT = 1.2
VIEW_TIMEOUT = 3.0

class Node:
    def __init__(self, node_id, priv_key, pub_key, network, f=1, all_nodes=None):
        self.id = node_id
        self.priv = priv_key
        self.pub = pub_key
        self.network = network
        self.state = NodeState(f=f)
        self.view = 0
        self.is_leader = False
        self.all_nodes = all_nodes or []

        self.world_state = WorldState()
        self.metrics = None
        self.did_pub_lookup = {}

        # ================================
        # 分开存储不同类型消息
        # ================================
        self.votes = {}          # {block_id: [vote_dict,...]}
        self.partial_sigs = {}   # {block_id: [partial_sig_dict,...]}
        self.soft_votes = {}     # {block_id: set(node_ids)}
        self.partial_qcs = {}    # {block_id: PartialQC}
        self.defer_notices = {}  # {view: set(node_ids)}

        self._soft_timer = None
        self._defer_timer = None
        self._view_timer = None

    # -----------------------------
    # helper: compute tx hash
    # -----------------------------
    @staticmethod
    def tx_hash(tx: dict) -> str:
        s = json.dumps({k: v for k, v in tx.items() if k != "signature"}, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(s.encode()).hexdigest()

    # -----------------------------
    # receive messages
    # -----------------------------
    def receive_message(self, msg_type, payload):
        if msg_type == "qc_announce":
            self.receive_message_qc_announce(payload)
        elif msg_type == "proposal":
            self.on_receive_proposal(payload)
        elif msg_type == "soft_vote":
            self.on_receive_soft_vote(payload)
        elif msg_type == "partial_signature":
            self.receive_partial_signature(payload)
        elif msg_type == "partial_qc":
            self.receive_partial_qc(payload)
        elif msg_type == "defer":
            self.on_receive_defer(payload)
        elif msg_type == "vote":
            self.on_receive_vote(payload)
        elif msg_type == "view_change":
            self.receive_view_change(payload)
        else:
            logging.warning(f"[{self.id}] Unknown msg_type {msg_type}")

    # -----------------------------
    # proposal
    # -----------------------------
    def on_receive_proposal(self, proposal):
        logging.debug(f"[{self.id}] Received proposal: {proposal}")
        block = proposal.get("block")
        qc = proposal.get("qc")
        view = proposal.get("view", 0)
        txs = proposal.get("transactions", [])

        if not self.validate_proposal(proposal):
            logging.warning(f"[{self.id}] Proposal invalid -> soft_vote")
            if not isinstance(qc, DummyQC):
                self.soft_vote(block, view)
            return

        # from transaction import verify_tx_signature
        # for tx in txs:
        #     if not verify_tx_signature(tx, self.did_pub_lookup):
        #         logging.warning(f"[{self.id}] Proposal contains invalid tx -> soft_vote")
        #         self.soft_vote(block, view)
        #         return

        # metrics
        if self.metrics:
            for tx in txs:
                self.metrics.mark_included(Node.tx_hash(tx))

        # store block locally
        self.state.add_block(block)
        block.payload = {"transactions": txs}

        # QC conflict check
        if qc and is_conflicting_qc(qc, getattr(self.state, "latest_qc", None)):
            logging.warning(f"[{self.id}] QC conflict detected -> soft_vote")
            self.soft_vote(block, view)
            return

        # 正常路径 -> vote
        self.vote(block, view)

    # -----------------------------
    # voting
    # -----------------------------
    def vote(self, block, view):
        sig = BLS.sign(self.priv, block.id.encode())
        vote = Vote(self.id, block.id, sig, view)
        self.votes.setdefault(block.id, [])
        self.votes[block.id].append(vote.to_dict())
        self.network.broadcast_vote(self.id, vote.to_dict())

        # broadcast partial signature
        self.broadcast_partial_signature(block, view)

    def soft_vote(self, block, view):
        self.soft_votes.setdefault(block.id, set())
        self.soft_votes[block.id].add(self.id)
        self.network.broadcast_soft_vote(self.id, block.id)
        self.broadcast_partial_signature(block, view)
        self.start_soft_timer(view)

    # def broadcast_partial_signature(self, block, view=None):
    #     view = view or block.view   # 保证 view 与 block.view 一致
    #     sig = BLS.sign(self.priv, f"partial:{block.id}:{view}")
    #     msg = {
    #         "block_id": block.id,
    #         "view": view,
    #         "high_qc_view": getattr(self.state.latest_qc, "view", 0),
    #         "signature": sig,
    #         "signer": self.id
    #     }
    #     self.partial_sigs.setdefault(block.id, [])
    #     if not any(p["signer"] == msg["signer"] for p in self.partial_sigs[block.id]):
    #         self.partial_sigs[block.id].append(msg)
    #     self.network.broadcast_partial_signature(self.id, msg)
    def broadcast_partial_signature(self, block, view=None):
        view = view or block.view
        # [FIX 1] 统一签名内容：与 validate_qc 中的验证内容保持一致 (block.id.encode())
        # 原代码: sig = BLS.sign(self.priv, f"partial:{block.id}:{view}")
        sig = BLS.sign(self.priv, block.id.encode()) 

        msg = {
            "block_id": block.id,
            "view": view,
            "high_qc_view": getattr(self.state.latest_qc, "view", 0),
            "signature": sig,
            "signer": self.id
        }
        
        # 本地存储一份
        self.partial_sigs.setdefault(block.id, [])
        if not any(p["signer"] == msg["signer"] for p in self.partial_sigs[block.id]):
            self.partial_sigs[block.id].append(msg)
            
        self.network.broadcast_partial_signature(self.id, msg)

    def on_receive_vote(self, vote_dict):
        # ---- 1. 获取 block_id ----
        bid = vote_dict.get("block_id")
        if not bid:
            logging.warning(f"[{self.id}] vote missing block_id: {vote_dict}")
            return

        # ---- 2. 获取 signer（兼容 voter_id）----
        signer = vote_dict.get("signer") or vote_dict.get("voter_id")
        if not signer:
            logging.warning(f"[{self.id}] vote missing signer/voter_id: {vote_dict}")
            return

        signature = vote_dict.get("signature")
        view = vote_dict.get("view", 0)

        normalized = {
            "block_id": bid,
            "signer": signer,
            "signature": signature,
            "view": view
        }

        # ---- 3. 确保结构存在 ----
        self.votes.setdefault(bid, [])

        # ---- 4. 去重 ----
        if any(v.get("signer") == signer for v in self.votes[bid]):
            return

        # ---- 5. 保存 ----
        self.votes[bid].append(normalized)

        logging.debug(f"[{self.id}] collected vote for {bid} from {signer} (total={len(self.votes[bid])})")

   
    def receive_partial_signature(self, msg):
        # logging.debug(f"[{self.id}] partial_sigs[{bid}] count={len(self.partial_sigs[bid])} signers={[p['signer'] for p in self.partial_sigs[bid]]}")
        bid = msg["block_id"]
        if bid is None or "signer" not in msg:
            return
        self.partial_sigs.setdefault(bid, [])
        
        # 去重
        if any(p["signer"] == msg["signer"] for p in self.partial_sigs[bid]):
            return
        self.partial_sigs[bid].append(msg)

        count = len(self.partial_sigs[bid])
        
        # === 替换 receive_partial_signature 里生成 PQC/QC 的代码段 ===
        if self.is_leader:
            logging.debug(f"[{self.id}] Collected {count} partials for {bid} (need {self.state.full_q_threshold})")
             # helper inside receive_partial_signature
            def _to_bytes(x):
                if x is None:
                    return None
                if isinstance(x, (bytes, bytearray)):
                    return bytes(x)
                if isinstance(x, str):
                    # 尝试 hex decode；若失败则 utf-8 encode
                    try:
                        return bytes.fromhex(x)
                    except Exception:
                        return x.encode()
                # fallback
                return str(x).encode()

            # make canonical copy and normalize sigs
            pairs = list(self.partial_sigs[bid])  # shallow copy
            # canonical orders to try:
            orders_to_try = []

            # order A: by signer id (deterministic)
            pairs_by_signer = sorted(pairs, key=lambda p: str(p.get("signer")))
            orders_to_try.append(("signer_sorted", pairs_by_signer))

            # order B: by network node order (self.all_nodes) but only include present signers
            try:
                node_order = list(self.all_nodes)
                pairs_by_netorder = sorted(
                    pairs,
                    key=lambda p: node_order.index(p.get("signer")) if p.get("signer") in node_order else len(node_order)
                )
                orders_to_try.append(("network_order", pairs_by_netorder))
            except Exception:
                # 如果 all_nodes 不可用就忽略
                pass

            # normalize signatures and build debug output
            for name, ps in orders_to_try:
                s_list = []
                sig_list = []
                for p in ps:
                    s = p.get("signer")
                    rawsig = p.get("signature")
                    bsig = _to_bytes(rawsig)
                    s_list.append(s)
                    sig_list.append(bsig)
                logging.debug(f"[{self.id}] DEBUG QC build for {bid} using order={name}: count={len(sig_list)}, signers={s_list}")
                if sig_list:
                    logging.debug(f"[{self.id}] first_sig_type={type(sig_list[0])} len={len(sig_list[0])} hex_prefix={sig_list[0][:8].hex()}")

            # Try verification attempts
            # Prepare canonical arrays for the first order (signer_sorted)
            pairs = pairs_by_signer
            signers = [p["signer"] for p in pairs]
            partials = [_to_bytes(p["signature"]) for p in pairs if p.get("signature") is not None]

            logging.debug(f"[{self.id}] attempt verify with signers={signers} partials_count={len(partials)}")

            # 1) try verify_aggregate(signers, msg, agg) as before after aggregate
            try:
                agg_candidate = aggregate(partials)
                logging.debug(f"[{self.id}] aggregated signature len={len(agg_candidate) if isinstance(agg_candidate,(bytes,bytearray)) else 'NA'}")
            except Exception as e:
                logging.exception(f"[{self.id}] aggregate failed: {e}")
                agg_candidate = None

            if agg_candidate:
                # attempt 1: treat signers as ids (original approach)
                try:
                    # ok = verify_aggregate(signers, bid.encode(), agg_candidate)
                    msg = bid.encode()  # 单条消息
                    msgs_list = [msg] * len(signers)  # 重复，长度与 pubkeys 一致
                    # ok = verify_aggregate(signers, msgs_list, agg_candidate)
                    # 统一使用 pubkeys
                    pubkeys = [_to_bytes(self.did_pub_lookup.get(s)) for s in signers]
                    msgs_list = [bid.encode()] * len(pubkeys)
                    ok = verify_aggregate(pubkeys, msgs_list, agg_candidate)
                    logging.debug(f"[{self.id}] verify_aggregate(signers...) returned {ok}")
                except Exception as e:
                    logging.debug(f"[{self.id}] verify_aggregate(signers...) exception: {e}")
                    ok = False

                # attempt 2: if failed, map signers -> pubkeys via did_pub_lookup and normalize
                if not ok and hasattr(self, "did_pub_lookup") and self.did_pub_lookup:
                    try:
                        pubkeys = []
                        for s in signers:
                            pk = self.did_pub_lookup.get(s, None)
                            if pk is None:
                                pubkeys.append(None)
                            else:
                                pubkeys.append(_to_bytes(pk))
                        logging.debug(f"[{self.id}] trying verify_aggregate with pubkeys types={[type(p) for p in pubkeys]} count={len(pubkeys)}")
                        # ok = verify_aggregate(pubkeys, bid.encode(), agg_candidate)
                        msg = bid.encode()  # 单条消息
                        msgs_list = [msg] * len(pubkeys)  # 重复，长度与 pubkeys 一致
                        pubkeys = [_to_bytes(self.did_pub_lookup.get(s)) for s in pubkeys]
                        msgs_list = [bid.encode()] * len(pubkeys)
                        ok = verify_aggregate(pubkeys, msgs_list, agg_candidate)
                        logging.debug(f"[{self.id}] verify_aggregate(pubkeys...) returned {ok}")
                    except Exception as e:
                        logging.debug(f"[{self.id}] verify_aggregate(pubkeys...) exception: {e}")
                        ok = False

                # attempt 3: if still failed, try alternate order (network order) if available
                if not ok and len(orders_to_try) > 1:
                    alt_pairs = orders_to_try[1][1]
                    alt_signers = [p["signer"] for p in alt_pairs]
                    alt_partials = [_to_bytes(p["signature"]) for p in alt_pairs if p.get("signature") is not None]
                    try:
                        agg_alt = aggregate(alt_partials)
                        logging.debug(f"[{self.id}] trying alternate order verify with signers={alt_signers}")
                        # ok = verify_aggregate(alt_signers, bid.encode(), agg_alt)
                        msg = bid.encode()  # 单条消息
                        msgs_list = [msg] * len(alt_signers)  # 重复，长度与 pubkeys 一致
                        pubkeys = [_to_bytes(self.did_pub_lookup.get(s)) for s in alt_signers]
                        msgs_list = [bid.encode()] * len(alt_signers)
                        ok = verify_aggregate(pubkeys, msgs_list, agg_alt)
                        logging.debug(f"[{self.id}] alt verify_aggregate(signers...) returned {ok}")
                        if not ok and hasattr(self, "did_pub_lookup") and self.did_pub_lookup:
                            pubkeys_alt = [_to_bytes(self.did_pub_lookup.get(s)) if self.did_pub_lookup.get(s) else None for s in alt_signers]
                            # ok = verify_aggregate(pubkeys_alt, bid.encode(), agg_alt)
                            msg = bid.encode()  # 单条消息
                            msgs_list = [msg] * len(pubkeys_alt)  # 重复，长度与 pubkeys 一致
                            ok = verify_aggregate(pubkeys_alt, msgs_list, agg_alt)
                            logging.debug(f"[{self.id}] alt verify_aggregate(pubkeys...) returned {ok}")
                    except Exception as e:
                        logging.debug(f"[{self.id}] alt verify exception: {e}")
                        ok = False
            else:
                ok = False

            # If all aggregate attempts failed, do per-signer single-check to find bad entries
            if not ok:
                logging.warning(f"[{self.id}] all aggregate attempts failed for {bid}; starting per-signer checks")
                for p in pairs:
                    s = p.get("signer")
                    sig = _to_bytes(p.get("signature"))
                    # try verifying this single signature by calling verify_aggregate with single-element arrays (works if verify_aggregate supports it)
                    try:
                        # try with signer id
                        ok_single = False
                        try:
                            ok_single = verify_aggregate([s], bid.encode(), sig)
                            logging.debug(f"[{self.id}] single verify with signer-id for {s} => {ok_single}")
                        except Exception as e:
                            logging.debug(f"[{self.id}] single verify with signer-id exception for {s}: {e}")
                        # try with pubkey
                        if not ok_single and hasattr(self, "did_pub_lookup") and self.did_pub_lookup.get(s):
                            pk = _to_bytes(self.did_pub_lookup.get(s))
                            try:
                                ok_single = verify_aggregate([pk], bid.encode(), sig)
                                logging.debug(f"[{self.id}] single verify with pubkey for {s} => {ok_single}")
                            except Exception as e:
                                logging.debug(f"[{self.id}] single verify with pubkey exception for {s}: {e}")

                        if not ok_single:
                            logging.warning(f"[{self.id}] signature from {s} failed single-check (len_sig={len(sig) if sig else 'None'})")
                    except Exception as e:
                        logging.debug(f"[{self.id}] per-signer check exception for {s}: {e}")

            # Final: if ok True proceed forming PQC/Full QC as before; otherwise log and don't broadcast
            if ok:
                # build and broadcast PQC / QC (same as before)
                qc = QC(
                    block_id=bid,
                    view=self.view,
                    signatures=partials,
                    agg=agg_candidate,
                    signers=signers  # 或者 pubkeys
                )
                self.state.update_latest_qc(qc)
                self.broadcast_qc(qc)
            else:
                logging.warning(f"[{self.id}] PartialQC/FULL QC verify failed for {bid} after exhaustive checks (count={len(partials)})")


    # -----------------------------
    # receive partial QC
    # -----------------------------
    def receive_partial_qc(self, msg):
        bid = msg["block_id"]
        pqc = PartialQC(
            block_id=bid,
            view=msg["view"],
            aggregated_sig=msg["aggregated_sig"],
            signer_count=msg["signer_count"]
        )
        self.partial_qcs[bid] = pqc
        logging.debug(f"[{self.id}] stored PartialQC for {bid}")

    def broadcast_qc(self, qc):
        data = {"block": self.state.get_block(qc.block_id), "qc": qc, "view": qc.view}
        self.network.broadcast(self.id, "qc_announce", data)

    # -----------------------------
    # qc announce
    # -----------------------------
    def receive_message_qc_announce(self, data):
        block = data.get("block")
        qc = data.get("qc")
        logging.info(f"[{self.id}] Received QC announce for block {qc.block_id}")
        self.state.update_latest_qc(qc)
        self.state.update_locked_qc(qc)
        if qc.view > getattr(self.state.commit_qc, "view", 0):
            logging.info(f"[{self.id}] Committing block {qc.block_id}")
            self.commit_block(qc.block_id)

    # -----------------------------
    # commit
    # -----------------------------
    def commit_block(self, block_id):
        logging.info(f"[{self.id}] COMMITTED block {block_id}")
        blk = self.state.get_block(block_id)
        if blk and getattr(blk, "payload", None):
            txs = blk.payload.get("transactions", [])
            for tx in txs:
                self.world_state.apply_transaction(tx)
                if self.metrics:
                    txid = Node.tx_hash(tx)
                    self.metrics.mark_committed(txid)
        self.state.commit_qc = self.state.latest_qc

    # -----------------------------
    # proposal/qc validation
    # -----------------------------
    def validate_proposal(self, proposal):##TODO1111
        # block = proposal.get("block")
        # qc = proposal.get("qc")
        # view = proposal.get("view", 0)

        # if not block:
        #     logging.warning(f"[{self.id}] Proposal missing block")
        #     return False
        # if view < self.view:
        #     logging.warning(f"[{self.id}] Proposal view {view} is stale")
        #     return False
        # if qc and not self.validate_qc(qc):
        #     logging.warning(f"[{self.id}] Proposal contains invalid QC")
        #     return False
        return True

    def validate_qc(self, qc):##TODO1111
        # if isinstance(qc, DummyQC):
        #     return True
        # if not verify_aggregate(qc.signers, qc.block_id.encode(), qc.agg):
        #     logging.warning(f"[{self.id}] QC signature verification failed for block {qc.block_id}")
        #     return False
        # if qc.view < getattr(self.state.latest_qc, "view", 0):
        #     logging.warning(f"[{self.id}] QC view {qc.view} is stale")
        #     return False
        return True

    # -----------------------------
    # soft/defer timers
    # -----------------------------
    def start_soft_timer(self, view):
        if self._soft_timer:
            self._soft_timer.cancel()
        def on_timeout():
            logging.debug(f"[{self.id}] soft timeout expired for v{view}, broadcasting defer")
            self.send_defer(view)
            self.start_defer_timer(view)
        self._soft_timer = threading.Timer(SOFT_VOTE_TIMEOUT, on_timeout)
        self._soft_timer.start()

    def send_defer(self, view):
        msg = {"sender": self.id, "view": view}
        self.network.broadcast(self.id, "defer", msg)
        self.defer_notices.setdefault(view, set()).add(self.id)

    def on_receive_defer(self, msg):
        view = msg["view"]
        sender = msg["sender"]
        self.defer_notices.setdefault(view, set()).add(sender)
        if len(self.defer_notices[view]) >= self.state.full_q_threshold:
            logging.warning(f"[{self.id}] collected >=2f+1 defers for v{view} -> trigger view change")
            self.handle_view_change(view+1)

    def start_defer_timer(self, view):
        if self._defer_timer:
            self._defer_timer.cancel()
        def on_defertout():
            logging.debug(f"[{self.id}] defer timer expired for v{view}, attempting view change")
            self.handle_view_change(view+1)
        self._defer_timer = threading.Timer(DEFER_TIMEOUT, on_defertout)
        self._defer_timer.start()

    # -----------------------------
    # view change
    # -----------------------------
    def handle_view_change(self, new_view):
        logging.info(f"[{self.id}] handling view change to v{new_view}")
        self.view = new_view
        leader_id = self.all_nodes[new_view % len(self.all_nodes)]
        self.is_leader = (self.id == leader_id)
        logging.info(f"[{self.id}] New leader for view {new_view} is {leader_id}")

        vc = {
            "new_view": new_view,
            "highest_qc": getattr(self.state.latest_qc, "to_dict", lambda: None)(),
            "partial_qcs": {k: v.to_dict() for k,v in self.partial_qcs.items()},
            "locked_block": getattr(self.state.locked_qc, "block_id", None),
            "sender": self.id
        }
        self.network.broadcast_view_change(self.id, vc)

    def receive_view_change(self, msg):
        logging.debug(f"[{self.id}] receive_view_change from {msg.get('sender')}, new_view={msg.get('new_view')}")

    # -----------------------------
    # soft vote receive (just log)
    # -----------------------------
    def on_receive_soft_vote(self, payload):
        logging.debug(f"[{self.id}] soft_vote received payload={payload}")
