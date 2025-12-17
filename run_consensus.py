# run_consensus.py - 统一启动脚本
import logging
import time
from network import Network
from node import Node
from consensus import Consensus
from crypto import generate_keys

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def create_nodes(network, num_nodes=4, f=1):
    """创建所有节点"""
    nodes = {}
    group_pk = b"dummy_group_pk"  # 应该从密码学生成
    
    for i in range(num_nodes):
        node_id = f"node{i}"
        
        # 生成密钥对
        priv_key, pub_key = generate_keys()
        did_priv, did_pub = i, f"did_pub_{i}"  # 简化的DID
        
        # 创建节点
        node = Node(
            node_id=node_id,
            index=i,
            priv_key=priv_key,
            pub_key=pub_key,
            network=network,
            group_pk=group_pk,
            did_priv=did_priv,
            did_pub=did_pub,
            f=f
        )
        nodes[node_id] = node
        
        # 注册到网络
        network.register_node(node)
        
        logger.info(f"创建节点: {node_id}, 索引={i}")
    
    return nodes

def main():
    """主启动函数"""
    logger.info("=== 启动分布式共识系统 ===")
    
    # 1. 创建网络
    network = Network()
    
    # 2. 创建节点
    nodes = create_nodes(network, num_nodes=4, f=1)
    
    # 3. 创建共识实例（需要传递所有节点）
    consensus = Consensus(
        nodes=nodes,
        network=network,
        f=1,
        max_rounds=10
    )
    
    # 4. 将共识实例设置给每个节点
    for node_id, node in nodes.items():
        node.consensus = consensus
        logger.info(f"为节点 {node_id} 设置共识实例")
    
    # 5. 启动共识主循环
    logger.info("启动共识主循环...")
    consensus.start()
    
    # 6. 等待共识完成
    try:
        while consensus.active and consensus.current_round < consensus.max_rounds:
            time.sleep(1)
            
            # 打印进度
            logger.info(f"共识进度: 轮次 {consensus.current_round}/{consensus.max_rounds}, "
                       f"视图 {consensus.view}")
            
            # 检查是否有节点失败
            active_nodes = [node_id for node_id, node in nodes.items() 
                          if hasattr(node, 'processing_active') and node.processing_active]
            logger.debug(f"活动节点: {len(active_nodes)}/{len(nodes)}")
        
        logger.info("共识循环完成")
        
    except KeyboardInterrupt:
        logger.info("收到中断信号，停止系统...")
        consensus.stop()
        
        # 停止所有节点的消息处理
        for node in nodes.values():
            node.processing_active = False
        
        logger.info("系统已停止")
    
    except Exception as e:
        logger.error(f"系统运行异常: {e}")
        raise

if __name__ == "__main__":
    main()