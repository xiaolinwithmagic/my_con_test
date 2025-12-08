import threading
import logging
from utils412 import Node, Network, Block, Mempool


def test_consensus(consensus_type, rounds=3, f=1):
    """
    测试共识流程
    :param consensus_type: 共识类型 "pbft" / "hotstuff"
    :param rounds: 共识轮数
    :param f: 容错数
    """
    # 1. 初始化节点（4个节点，f=1 满足 3f+1=4）
    node_ids = ["node_0", "node_1", "node_2", "node_3"]
    nodes = {nid: Node(nid) for nid in node_ids}

    # 2. 初始化共识实例和网络层
    if consensus_type == "pbft":
        from PBFT import PBFTConsensus
        consensus = PBFTConsensus(nodes, None, f=f)
    elif consensus_type == "hotstuff":
        from HotStuff import HotStuffConsensus
        consensus = HotStuffConsensus(nodes, None, f=f)
    else:
        raise ValueError("consensus_type must be 'pbft' or 'hotstuff'")
    
    # 绑定网络层
    network = Network(consensus)
    consensus.network = network

    # 3. 启动共识并等待执行完成
    logging.info(f"\n========== Testing {consensus_type.upper()} Consensus ==========")
    t = consensus.start(rounds=rounds)
    t.join(timeout=(rounds * (consensus.proposal_interval + 0.5) + 1))  # 等待共识结束

    # 4. 验证结果
    total_committed = 0
    for nid, node in nodes.items():
        committed_num = len(node.state.committed_blocks)
        total_committed += committed_num
        logging.info(f"Node {nid} committed blocks: {committed_num} | IDs: {list(node.state.committed_blocks)}")
    
    avg_committed = total_committed / len(nodes)
    logging.info(f"\n[TEST RESULT] {consensus_type.upper()} - Average committed blocks per node: {avg_committed}")
    # assert avg_committed >= rounds * 0.8, f"{consensus_type} consensus failed: too few blocks committed"
    # logging.info(f"{consensus_type.upper()} test PASSED!\n")

# 主测试入口
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    # 测试PBFT
    test_consensus("pbft", rounds=3, f=1)
    # 测试HotStuff
    test_consensus("hotstuff", rounds=3, f=1)