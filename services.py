# 创建一个 services.py 文件
import logging
from typing import Dict, List, Optional

# 创建当前模块的logger实例（推荐方式，而非直接用logging.root）
logger = logging.getLogger(__name__)


# services.py —— 纯无依赖容器，不导入任何业务模块！
class Services:
    """纯服务容器，仅做存储，不导入/依赖任何业务类"""
    def __init__(self):
        self.network = None
        self.nodes = {}
        self.consensus = None
        self.mempool = None
        self.simulator = None  # 新增：存储模拟器
    
    def register(self, name, instance):
        setattr(self, name, instance)
    
    def get(self, name):
        return getattr(self, name, None)

# 全局单例（仅导出这个实例，无其他依赖）
services = Services()