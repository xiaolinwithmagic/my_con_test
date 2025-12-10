# mempool.py (修复版)
import queue
import threading
import time
from typing import Optional, Dict, List
import hashlib
import json

class Mempool:
    def __init__(self):
        # 使用 queue.Queue 作为主要数据结构
        self.queue = queue.Queue()
        self.lock = threading.Lock()
        self.seen = set()  # 防止重复交易（基于交易ID）
        
        # 可选：用于快速访问的列表（如果需要）
        self.all_txs = []  # 所有交易列表（用于调试）
        
    def push(self, tx: Dict) -> bool:
        """添加交易到内存池"""
        txid = self._txid(tx)
        
        with self.lock:
            if txid in self.seen:
                return False  # 交易已存在
                
            self.seen.add(txid)
            self.queue.put(tx)
            self.all_txs.append(tx)  # 可选：添加到列表
            return True
    
    def pop(self, timeout: float = 0.1) -> Optional[Dict]:
        """从内存池取出一个交易"""
        try:
            tx = self.queue.get(timeout=timeout)
            
            # 从seen中移除（如果交易被取出，可以重新加入）
            txid = self._txid(tx)
            with self.lock:
                self.seen.discard(txid)  # 使用discard避免KeyError
                
            return tx
        except queue.Empty:
            return None
    
    def get_txs(self, n: int, timeout: float = 0.1) -> List[Dict]:
        """取 n 笔交易用于打包"""
        txs = []
        
        for _ in range(n):
            tx = self.pop(timeout=timeout)
            if tx is None:
                break  # 没有更多交易
            txs.append(tx)
        
        return txs
    
    def size(self) -> int:
        """返回内存池中交易数量"""
        return self.queue.qsize()
    
    def __len__(self) -> int:
        """支持 len(mempool) 语法"""
        return self.size()
    
    def clear(self) -> None:
        """清空内存池"""
        with self.lock:
            while not self.queue.empty():
                try:
                    self.queue.get_nowait()
                except queue.Empty:
                    break
            self.seen.clear()
            self.all_txs.clear()
    
    def _txid(self, tx: Dict) -> str:
        """计算交易ID（排除签名字段）"""
        # 创建副本并移除签名
        tx_copy = dict(tx)
        tx_copy.pop("signature", None)
        
        # 稳定序列化
        s = json.dumps(tx_copy, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(s.encode()).hexdigest()
    
    def stats(self) -> Dict:
        """返回内存池统计信息"""
        return {
            "size": self.size(),
            "unique_txs": len(self.seen),
            "all_txs_count": len(self.all_txs)
        }
    
    def peek(self, n: int = 10) -> List[Dict]:
        """查看但不取出交易（用于调试）"""
        # 注意：queue.Queue 不支持直接查看，所以我们用 all_txs
        with self.lock:
            return self.all_txs[:n]