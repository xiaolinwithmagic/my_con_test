# quick_test.py
import time
from block import Block, create_genesis_block
from qc import QC
from state import NodeState
from mempool import Mempool
from network import Network
from node import Node

print("最小共识测试")
print("=" * 50)

# 1. 创建网络和内存池
net = Network(drop_rate=0.0, delay_range=(0, 0))
mempool = Mempool()

# 2. 创建2个节点
nodes = {}
for i in range(2):
    node_id = f"node{i}"
    node = Node(
        node_id=node_id,
        index=i,
        priv_key=f"priv_{i}".encode(),
        pub_key=f"pub_{i}".encode(),
        network=net,
        f=0,  # f=0简化测试
        all_nodes=["node0", "node1"]
        # did_priv=ident["priv"],
        # did_pub=ident["pub"],
        # group_pk=self.group_pk
    )
    nodes[node_id] = node
    net.register(node)

# 3. 初始化创世区块
genesis = create_genesis_block()
genesis_qc = QC(
    view=0,
    block_hash=genesis.hash,
    aggregate_signature=b"genesis",
    signer_bitmap=0,
    merkle_root=bytes([0]*32)
)

for node in nodes.values():
    node.state.add_block(genesis)
    node.state.update_latest_qc(genesis_qc)

print(f"创世区块: {genesis.id[:8]}")
print(f"节点数: {len(nodes)}")

# 4. 手动模拟一轮共识
print("\n模拟第一轮共识:")
view = 1
leader_id = "node0"  # 视图1的leader
leader = nodes[leader_id]

# Leader创建区块
block = Block(
    parent_hash=leader.state.latest_qc.block_hash,
    height=1,
    proposer=leader_id,
    payload=["test_tx"],
    qc=leader.state.latest_qc,
    view=view
)
leader.state.add_block(block)
print(f"Leader {leader_id}创建区块: {block.id[:8]}")

# 假设所有副本投票
print("所有副本投票...")

# 形成QC（简化，跳过真实签名）
qc = QC(
    view=view,
    block_hash=block.hash,
    aggregate_signature=b"mock_agg_sig",
    signer_bitmap=0b11,  # 两个节点都签名
    merkle_root=bytes([1]*32)
)

# 更新所有节点状态
for node in nodes.values():
    node.state.update_latest_qc(qc)
    node.state.update_locked_qc(qc)

print(f"形成QC: view={qc.view}, signers={qc.get_signer_count()}")

# 提交区块
for node in nodes.values():
    node.state._try_commit_blocks()

print("\n" + "=" * 50)
print("手动模拟完成！")
print(f"node0状态: height={nodes['node0'].state.height}")
print(f"node1状态: height={nodes['node1'].state.height}")
print("=" * 50)