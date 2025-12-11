import json
from typing import Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum
import logging

logger = logging.getLogger(__name__)


# 定义交易类型枚举
class TransactionType(Enum):
    REGISTER = "REGISTER"
    TRANSFER = "TRANSFER"
    LICENSE = "LICENSE"

# 定义交易数据类
@dataclass
class Transaction:
    type: TransactionType
    image_hash: str
    data: Dict[str, Any]
    
    @classmethod
    def from_dict(cls, tx_dict: Dict[str, Any]) -> 'Transaction':
        """从字典创建交易"""
        if not isinstance(tx_dict, dict):
            raise ValueError(f"Transaction must be dict, got {type(tx_dict)}")
        
        ttype = tx_dict.get("type")
        if not ttype:
            raise ValueError("Transaction missing 'type' field")
        
        image_hash = tx_dict.get("image_hash")
        if not image_hash:
            raise ValueError("Transaction missing 'image_hash' field")
        
        return cls(
            type=TransactionType(ttype),
            image_hash=image_hash,
            data=tx_dict
        )
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Transaction':
        """从JSON字符串创建交易"""
        try:
            tx_dict = json.loads(json_str)
            return cls.from_dict(tx_dict)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON: {e}")
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return self.data.copy()

# 世界状态类
class WorldState:
    def __init__(self):
        """
        初始化世界状态
        state结构:
        {
            "image_hash1": {
                "owner": "did1",           # 当前所有者DID
                "licensees": [             # 被许可人列表
                    {"licensee": "did2", "terms": {...}},
                    ...
                ],
                "history": [               # 交易历史
                    {交易数据1},
                    {交易数据2},
                    ...
                ]
            },
            ...
        }
        """
        self.state: Dict[str, Dict[str, Any]] = {}
        self.logger = logging.getLogger(__name__)
    
    def reset(self):
        """重置世界状态"""
        self.state.clear()
        self.logger.info("World state reset")
    
    def get_image_info(self, image_hash: str) -> Optional[Dict[str, Any]]:
        """获取图片信息"""
        return self.state.get(image_hash)
    
    def normalize_transaction(self, tx_input) -> Transaction:
        """
        规范化交易输入
        支持: 字典, JSON字符串, Transaction对象
        """
        if isinstance(tx_input, Transaction):
            return tx_input
        
        if isinstance(tx_input, dict):
            return Transaction.from_dict(tx_input)
        
        if isinstance(tx_input, str):
            return Transaction.from_json(tx_input)
        
        raise ValueError(f"Unsupported transaction input type: {type(tx_input)}")
    
    def apply_transaction(self, tx_input) -> bool:
        """
        应用交易到世界状态
        返回: True表示成功，False表示失败
        """
        try:
            # 1. 规范化交易
            tx = self.normalize_transaction(tx_input)
            
            # 2. 根据交易类型处理
            if tx.type == TransactionType.REGISTER:
                return self._handle_register(tx)
            elif tx.type == TransactionType.TRANSFER:
                return self._handle_transfer(tx)
            elif tx.type == TransactionType.LICENSE:
                return self._handle_license(tx)
            else:
                self.logger.warning(f"Unknown transaction type: {tx.type}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error applying transaction: {e}")
            self.logger.debug(f"Transaction input: {tx_input}")
            return False
    
    def _handle_register(self, tx: Transaction) -> bool:
        """处理注册交易"""
        ih = tx.image_hash
        tx_dict = tx.to_dict()
        
        # 检查必填字段
        if "creator_did" not in tx_dict:
            self.logger.error("REGISTER transaction missing 'creator_did'")
            return False
        
        # 如果未注册，创建新记录
        if ih not in self.state:
            self.state[ih] = {
                "owner": tx_dict["creator_did"],
                "licensees": [],
                "history": [tx_dict]
            }
            self.logger.debug(f"Registered image {ih} for {tx_dict['creator_did']}")
            return True
        else:
            # 已注册，添加到历史记录
            self.state[ih]["history"].append(tx_dict)
            self.logger.warning(f"Image {ih} already registered, added to history")
            return False
    
    def _handle_transfer(self, tx: Transaction) -> bool:
        """处理转移交易"""
        ih = tx.image_hash
        tx_dict = tx.to_dict()
        
        # 检查必填字段
        if "from_did" not in tx_dict or "to_did" not in tx_dict:
            self.logger.error("TRANSFER transaction missing 'from_did' or 'to_did'")
            return False
        
        # 检查所有权
        if ih in self.state and self.state[ih]["owner"] == tx_dict["from_did"]:
            # 执行转移
            self.state[ih]["owner"] = tx_dict["to_did"]
            self.state[ih]["history"].append(tx_dict)
            self.logger.debug(f"Transferred image {ih} from {tx_dict['from_did']} to {tx_dict['to_did']}")
            return True
        else:
            # 无效转移，创建占位记录
            self.state.setdefault(ih, {
                "owner": None,
                "licensees": [],
                "history": []
            })["history"].append(tx_dict)
            self.logger.warning(f"Invalid transfer for image {ih}")
            return False
    
    def _handle_license(self, tx: Transaction) -> bool:
        """处理许可交易"""
        ih = tx.image_hash
        tx_dict = tx.to_dict()
        
        # 检查必填字段
        if "owner_did" not in tx_dict or "licensee_did" not in tx_dict:
            self.logger.error("LICENSE transaction missing 'owner_did' or 'licensee_did'")
            return False
        
        # 检查所有权
        if ih in self.state and self.state[ih]["owner"] == tx_dict["owner_did"]:
            # 添加被许可人
            license_info = {
                "licensee": tx_dict["licensee_did"],
                "terms": tx_dict.get("terms")
            }
            self.state[ih]["licensees"].append(license_info)
            self.state[ih]["history"].append(tx_dict)
            self.logger.debug(f"Licensed image {ih} from {tx_dict['owner_did']} to {tx_dict['licensee_did']}")
            return True
        else:
            # 无效许可
            self.state.setdefault(ih, {
                "owner": None,
                "licensees": [],
                "history": []
            })["history"].append(tx_dict)
            self.logger.warning(f"Invalid license for image {ih}")
            return False
    
    def apply_batch(self, transactions) -> Dict[str, Any]:
        """
        批量应用交易
        返回: {"success": 成功数量, "failed": 失败数量, "errors": 错误列表}
        """
        results = {
            "success": 0,
            "failed": 0,
            "errors": []
        }
        
        if not transactions:
            return results
        
        for i, tx_input in enumerate(transactions):
            try:
                success = self.apply_transaction(tx_input)
                if success:
                    results["success"] += 1
                else:
                    results["failed"] += 1
                    results["errors"].append({
                        "index": i,
                        "transaction": str(tx_input)[:100],  # 只取前100字符
                        "error": "Transaction application failed"
                    })
            except Exception as e:
                results["failed"] += 1
                results["errors"].append({
                    "index": i,
                    "transaction": str(tx_input)[:100],
                    "error": str(e)
                })
        
        return results
    
    def get_state_summary(self) -> Dict[str, Any]:
        """获取状态摘要"""
        total_images = len(self.state)
        total_transactions = sum(len(img["history"]) for img in self.state.values())
        
        return {
            "total_images": total_images,
            "total_transactions": total_transactions,
            "images": list(self.state.keys())
        }
    
    def export_state(self) -> Dict[str, Any]:
        """导出完整状态"""
        return self.state.copy()

# 使用示例
def example_usage():
    """使用示例"""
    ws = WorldState()
    
    # 创建交易
    register_tx = {
        "type": "REGISTER",
        "image_hash": "abc123",
        "creator_did": "did:owner:123"
    }
    
    transfer_tx = {
        "type": "TRANSFER",
        "image_hash": "abc123",
        "from_did": "did:owner:123",
        "to_did": "did:owner:456"
    }
    
    license_tx = {
        "type": "LICENSE",
        "image_hash": "abc123",
        "owner_did": "did:owner:456",
        "licensee_did": "did:licensee:789",
        "terms": {"duration": "1 year"}
    }
    
    # 应用交易
    print("Register:", ws.apply_transaction(register_tx))
    print("Transfer:", ws.apply_transaction(transfer_tx))
    print("License:", ws.apply_transaction(license_tx))
    
    # 查看状态
    print("\nImage info:", ws.get_image_info("abc123"))
    print("\nState summary:", ws.get_state_summary())