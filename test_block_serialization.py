 
from block import Block
from typing import Dict, Any, Optional
from qc import QC
from utils.logger import logger
from crypto import BLS
from time import time
from typing import Dict, Any
import json
import hashlib
import sys
import traceback
import copy



def test_block_serialization():
    """测试区块序列化和反序列化"""
    from qc import QC
    
    # 创建一个测试QC
    qc = QC(
        view=1,
        block_hash=b"test_hash" * 4,  # 32字节
        aggregate_signature=b"test_sig" * 6,  # 48字节
        signer_bitmap=0b111,
        merkle_root=b"merkle_root" * 3  # 32字节
    )
    
    # 创建区块
    block = Block(
        parent_hash=b"parent_hash" * 3,  # 32字节
        height=1,
        proposer="test_proposer",
        payload=["tx1", "tx2", "tx3"],
        qc=qc,
        view=1
    )
    
    print(f"Original block: {block}")
    print(f"Block hash: {block.hash.hex()[:16]}")
    
    # 序列化
    block_dict = block.to_dict()
    print(f"Serialized dict keys: {list(block_dict.keys())}")
    
    # 反序列化
    try:
        deserialized_block = Block.from_dict(block_dict)
        print(f"Deserialized block: {deserialized_block}")
        print(f"Deserialized hash: {deserialized_block.hash.hex()[:16]}")
        
        # 验证
        if block.hash == deserialized_block.hash:
            print("✓ Serialization test PASSED")
            return True
        else:
            print("✗ Serialization test FAILED: hash mismatch")
            return False
            
    except Exception as e:
        print(f"✗ Serialization test FAILED: {e}")
        return False

# 在脚本末尾添加
if __name__ == "__main__":
    test_block_serialization()