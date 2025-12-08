# mempool.py
import queue
import threading
from typing import Optional, Dict

class Mempool:
    def __init__(self):
        self.q = queue.Queue()
        self.lock = threading.Lock()
        self.seen = set()  # 防止重复 tx（tx_id）

    def push(self, tx: Dict):
        txid = self._txid(tx)
        with self.lock:
            if txid in self.seen:
                return False
            self.seen.add(txid)
            self.q.put(tx)
            return True

    def pop(self, timeout: float = 0.1) -> Optional[Dict]:
        try:
            return self.q.get(timeout=timeout)
        except queue.Empty:
            return None

    def size(self) -> int:
        return self.q.qsize()
    
    def add_tx(self, tx):
        with self.lock:
            self.pool.append(tx)

    def get_txs(self, n):
        """取 n 笔交易用于打包."""
        txs = []
        with self.lock:
            while len(txs) < n and self.pool:
                txs.append(self.pool.popleft())
        return txs

    def _txid(self, tx: Dict) -> str:
        import hashlib, json
        tt = dict(tx)
        tt.pop("signature", None)
        s = json.dumps(tt, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(s.encode()).hexdigest()
