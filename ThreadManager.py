import threading
import logging

logger = logging.getLogger(__name__)


class ThreadManager:
    """统一的线程管理器"""
    def __init__(self):
        self.threads = {}
        self.locks = {}
        self.running = True
    
    def register_thread(self, name: str, thread: threading.Thread):
        """注册线程"""
        self.threads[name] = thread
        logger.debug(f"[ThreadManager] 注册线程: {name}")
    
    def create_lock(self, name: str) -> threading.Lock:
        """创建锁"""
        lock = threading.Lock()
        self.locks[name] = lock
        return lock
    
    def stop_all(self):
        """停止所有线程"""
        self.running = False
        logger.info("[ThreadManager] 停止所有线程")
        
        # 等待所有线程结束（非守护线程）
        for name, thread in self.threads.items():
            if thread and thread.is_alive():
                logger.debug(f"[ThreadManager] 等待线程 {name} 结束")
                thread.join(timeout=2.0)
        
        logger.info("[ThreadManager] 所有线程已停止")