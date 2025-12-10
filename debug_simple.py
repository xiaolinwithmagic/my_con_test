# debug_simple.py
from block import Block, create_genesis_block
from qc import QC
from state import NodeState

print("=" * 60)
print("简单调试测试")
print("=" * 60)

# 1. 测试创世区块
genesis = create_genesis_block()
print(f"1. 创世区块: id={genesis.id[:8]}, hash={genesis.hash.hex()[:8]}")

# 2. 测试创世QC
genesis_qc = QC(
    view=0,
    block_hash=genesis.hash,
    aggregate_signature=b"genesis_sig",
    signer_bitmap=0,
    merkle_root=bytes([0]*32)
)
print(f"2. 创世QC: view={genesis_qc.view}, signer_count={genesis_qc.get_signer_count()}")

# 3. 测试节点状态
state = NodeState("test_node", f=1)
state.add_block(genesis)
state.update_latest_qc(genesis_qc)
print(f"3. 节点状态: height={state.height}, latest_qc.view={state.latest_qc.view}")

# 4. 创建第一个普通区块
block1 = Block(
    parent_hash=genesis.hash,
    height=1,
    proposer="node0",
    payload=["tx1", "tx2"],
    qc=genesis_qc,
    view=1
)
print(f"4. 区块1: id={block1.id[:8]}, height={block1.height}, view={block1.view}")

print("\n" + "=" * 60)
print("基础数据结构测试通过!")
print("=" * 60)