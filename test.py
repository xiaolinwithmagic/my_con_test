# test_run.py
import time
from simulator import Simulator

def main():
    print("=" * 60)
    print("HotStuff共识模拟器 - 简化测试")
    print("=" * 60)
    
    # 创建模拟器（减少节点数简化测试）
    sim = Simulator(
        num_nodes=5,  # 只用2个节点简化测试
        f=1,
        txs_per_proposal=5,
        drop_rate=0.0,  # 无丢包
        delay_range=(0.01, 0.02)  # 很小的延迟
    )
    
    print("\n[1/4] 设置模拟器...")
    sim.setup()
    print(f"  已创建 {len(sim.nodes)} 个节点")
    
    print("\n[2/4] 填充内存池...")
    sim.fill_mempool_registers(count=50)
    print(f"  内存池中有约 {50} 个交易")
    
    print("\n[3/4] 启动共识...")
    # 只运行几轮看看是否工作
    thread = sim.run(rounds=5, attack_type="none")
    
    print("\n[4/4] 等待共识运行...")
    print("  按 Ctrl+C 停止")
    
    try:
        # 等待一段时间观察输出
        for i in range(30):  # 最多等待30秒
            if not thread.is_alive():
                print(f"\n共识线程已停止")
                break
            
            # 打印一些状态
            if i % 5 == 0:  # 每5秒打印一次
                if hasattr(sim, 'consensus') and sim.consensus:
                    print(f"  运行中... 当前视图: {sim.consensus.view}, 轮数: {sim.current_round}")
            
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n\n用户中断")
    finally:
        print("\n停止模拟器...")
        sim.stop()
        time.sleep(1)  # 给停止操作一点时间
        
    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)

if __name__ == "__main__":
    main()