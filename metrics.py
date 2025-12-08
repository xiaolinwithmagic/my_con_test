# class Metrics:
#     def __init__(self):
#         self.fork_count = 0
#         self.invalid_qc_rate = 0
#         self.state_divergence = 0

#     def record_fork(self):
#         self.fork_count += 1


# metrics.py
import time
import threading
from collections import defaultdict

class Metrics:
    def __init__(self):
        self.lock = threading.Lock()
        self.tx_create_time = {}   # txid -> create_time
        self.tx_included_time = {} # txid -> included_in_block_time
        self.tx_committed_time = {}# txid -> committed_time
        self.committed_count = 0

    def mark_created(self, txid):
        with self.lock:
            self.tx_create_time[txid] = time.time()

    def mark_included(self, txid):
        with self.lock:
            self.tx_included_time[txid] = time.time()

    def mark_committed(self, txid):
        with self.lock:
            self.tx_committed_time[txid] = time.time()
            self.committed_count += 1

    def get_latency_stats(self):
        """
        返回 latency（inclusion_latency, commit_latency）均值（秒）
        inclusion_latency = included_time - create_time
        commit_latency = committed_time - create_time
        仅统计有数据的 tx
        """
        with self.lock:
            ins = []
            com = []
            for txid, ct in self.tx_create_time.items():
                it = self.tx_included_time.get(txid)
                jt = self.tx_committed_time.get(txid)
                if it:
                    ins.append(it - ct)
                if jt:
                    com.append(jt - ct)
            def mean(xs): return sum(xs)/len(xs) if xs else None
            return {"inclusion_mean": mean(ins), "commit_mean": mean(com), "committed_count": self.committed_count}
