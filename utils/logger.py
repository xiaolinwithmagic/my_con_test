
import json
import time
import logging

logger = logging.getLogger("consensus_trace")
logger.setLevel(logging.INFO)
handler = logging.FileHandler("trace1.log")   # 统一日志文件
formatter = logging.Formatter("%(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

def event(
    name: str, 
    node_id: str, 
    view: int = None, 
    block_id: str = None, 
    extra: dict = None,
    consensus_type: str = None  # 新增：共识类型（PBFT/HotStuff）
):
    """统一结构化埋点（新增共识类型）"""
    data = {
        "ts": time.time(),
        "event": name,
        "node": node_id,
    }

    if view is not None:
        data["view"] = view
    if block_id is not None:
        data["block_id"] = block_id
    if consensus_type is not None:  # 新增：写入共识类型
        data["consensus_type"] = consensus_type
    if extra:
        data.update(extra)

    logger.info(json.dumps(data))


