
import logging
from simulator import Simulator

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
# import logging
# logging.basicConfig(
#     level=logging.info
# )


def main():
    try:
        logger.info("创建模拟器...")
        # 1. 创建模拟器
        simulator = Simulator(num_nodes=4, f=0)
        
        # 2. 设置
        simulator.setup()
        simulator.fill_mempool_registers(count=100)
        
        # 3. 运行并获取线程对象
        logger.info("启动共识...")
        consensus_thread = simulator.run(rounds=2)
        
        # 4. 等待共识线程完成
        logger.info("等待共识完成...")
        consensus_thread.join(timeout=30)  # 最多等待30秒
        
        if consensus_thread.is_alive():
            logger.info("警告：共识线程超时，强制停止")
            simulator.stop()
        else:
            logger.info("共识线程正常结束")
        
        # 5. 验证安全机制
        results = simulator.verify_security_mechanisms()
        logger.info("\n安全机制验证结果:")
        for mechanism, passed in results.items():
            logger.info(f"  {mechanism}: {'✓ 通过' if passed else '✗ 失败'}")
        
        logger.info("Simulation completed successfully.")
        
    except KeyboardInterrupt:
        logger.info("\n模拟被用户中断")
        if 'simulator' in locals():
            simulator.stop()
    except Exception as e:
        logger.error(f"An error occurred during simulation: {e}", exc_info=True)

if __name__ == "__main__":
    main()