# import logging
# from network import Network
# from node import Node
# from consensus import Consensus
# from crypto import BLS

# logging.basicConfig(level=logging.INFO)


# def make_privpub():
#     """ 使用 BLS 生成密钥对 """
#     return BLS.generate_keypair()


# def main():
#     # 配置
#     f = 1
#     n = 4
#     node_ids = [f"node{i}" for i in range(n)]
#     net = Network(drop_rate=0.05, delay_range=(0.01, 0.05))

#     # 初始化节点
#     nodes = {}
#     for nid in node_ids:
#         priv, pub = make_privpub()
#         node = Node(
#             node_id=nid,
#             priv_key=priv,
#             pub_key=pub,
#             network=net,
#             f=f,
#             all_nodes=node_ids
#         )
#         nodes[nid] = node
#         net.register(node)
#         logging.info(f"[Init] Node {nid} started")

#     # 初始化共识模块
#     cons = Consensus(nodes, net, f=f)

#     # 启动共识
#     try:
#         t = cons.start(rounds=6)
#         logging.info("[Consensus] started")
#         t.join(timeout=20)
#     except Exception as e:
#         logging.error(f"[Fatal] Consensus error: {e}")
#     finally:
#         logging.info("[System] shutdown")


# if __name__ == "__main__":
#     main()

import logging
from simulator import Simulator

# # 配置日志
# logger = logging.getLogger()
# logger.setLevel(logging.DEBUG)

# # 创建文件处理器
# file_handler = logging.FileHandler("simulation.log", mode='w')
# file_handler.setLevel(logging.DEBUG)
# file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
# file_handler.setFormatter(file_formatter)
# logger.addHandler(file_handler)

# # 创建控制台处理器
# console_handler = logging.StreamHandler()
# console_handler.setLevel(logging.DEBUG)
# console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
# console_handler.setFormatter(console_formatter)
# logger.addHandler(console_handler)

# 1. 配置root logger（所有日志的顶层logger，会捕获所有未指定logger的输出）
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)  # 允许root接收所有级别日志
root_logger.handlers.clear()  # 清空默认handler，避免重复输出

# 2. 配置FileHandler（写入文件，捕获全量日志）
file_handler = logging.FileHandler("simulation.log", mode='w', encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(file_formatter)
root_logger.addHandler(file_handler)

# 3. 配置StreamHandler（输出到控制台，格式与文件一致）
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(file_formatter)
root_logger.addHandler(console_handler)

# 4. 自定义logger沿用root的配置（可选，确保main.py中的日志也统一）
logger = logging.getLogger(__name__)

def main():
    try:
        # 初始化模拟器
        logger.info("Initializing simulator...")
        simulator = Simulator(num_nodes=5)  # 设置节点数量为 4

        # 设置模拟器
        logger.info("Setting up simulator...")
        simulator.setup()

        # 填充交易池
        logger.info("Filling mempool with 200 register transactions...")
        simulator.fill_mempool_registers(10)  # 填充 200 个注册交易

        # 运行模拟器
        logger.info("Running simulator for 50 rounds...")
        t = simulator.run(rounds=5, attack=False)  # 运行 50 轮，无攻击
        t.join()  # 等待模拟器运行完成

        logger.info("Simulation completed successfully.")
    except Exception as e:
        logger.error(f"An error occurred during simulation: {e}", exc_info=True)

if __name__ == "__main__":
    main()