
import logging
from simulator import Simulator

# 1. 配置root logger（所有日志的顶层logger，会捕获所有未指定logger的输出）
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)  # 允许root接收所有级别日志
root_logger.handlers.clear()  # 清空默认handler，避免重复输出

# 2. 配置FileHandler（写入文件，捕获全量日志）
file_handler = logging.FileHandler("simulation1.log", mode='w', encoding='utf-8')
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
    logger.info("创建模拟器...")
    # 创建模拟器
    simulator = Simulator(
        num_nodes=4,
        f=1,
        txs_per_proposal=10,
        drop_rate=0.05,
        delay_range=(0.01, 0.05)
    )
    
    try:
        # 设置模拟器
        simulator.setup()
        
        # 填充内存池
        simulator.fill_mempool_registers(count=100)
        
        # 运行模拟器
        logger.info("启动模拟器...")
        sim_thread = simulator.run(
            rounds=50,
            attack_type="view_number_attack",
            attack_round=10
        )
        
        # 等待模拟完成（最多60秒）
        if not simulator.wait_for_completion(timeout=60):
            logger.warning("模拟超时，强制停止")
            simulator.stop()
        
        # 验证安全机制
        results = simulator.verify_security_mechanisms()
        logger.info("\n安全机制验证结果:")
        for mechanism, passed in results.items():
            logger.info(f"  {mechanism}: {'✓ 通过' if passed else '✗ 失败'}")
            
    except KeyboardInterrupt:
        logger.info("\n模拟被用户中断")
        simulator.stop()
        
    except Exception as e:
        logger.error(f"模拟异常: {e}", exc_info=True)
        simulator.stop()

if __name__ == "__main__":
    main()