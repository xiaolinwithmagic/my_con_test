# simulator.py (完整修改版)
import json
import time
import hashlib
import threading
import random
from typing import List, Dict, Any, Optional
from did_sim import generate_multiple_identities
from asset_sim import generate_dummy_hashes
from transaction import create_register_tx, create_transfer_tx, create_license_tx, tx_id
from mempool import Mempool
from metrics import Metrics
from network import Network
from node import Node
from consensus import Consensus 
from block import Block, create_genesis_block
from qc import QC
from transaction import verify_tx_signature
from utils.logger import event
from crypto import BLS
import logging
from typing import Dict, List, Optional

# 创建当前模块的logger实例（推荐方式，而非直接用logging.root）
logger = logging.getLogger(__name__)

class Simulator:
    def __init__(self, num_nodes, f, txs_per_proposal=20, drop_rate=0.05, delay_range=(0.01, 0.05)):
        self.num_nodes = num_nodes
        self.f = f
        self.tx_per_prop = txs_per_proposal
        self.drop_rate = drop_rate
        self.delay_range = delay_range
        
        # 网络和节点
        self.net = Network(drop_rate=drop_rate, delay_range=delay_range)
        self.nodes: Dict[str, Node] = {}
        self.node_ids: List[str] = []
        
        # 系统组件
        self.mempool = Mempool()
        self.metrics = Metrics()
        self.consensus: Optional[Consensus] = None
        
        # DID和资产
        self.identities, self.did_pub = generate_multiple_identities(max(50, num_nodes + 10))
        self.assets = generate_dummy_hashes(1000)
        
        # 运行状态
        self.active = False
        self.current_round = 0
        self.max_rounds = 0
        
        # 攻击配置
        self.attack_config = {
            "enabled": False,
            "attack_round": 0,
            "attack_type": "none"  # "split_proposal", "view_number_attack", "fake_bitmap"
        }
        
        # 创世区块
        self.genesis_block = create_genesis_block()
    
    def setup(self):
        self.node_ids = [f"node{i}" for i in range(self.num_nodes)]
        event("simulator_setup_start", node_id="system", view=0,
            consensus_type="my")
        
        self._generate_identities()
        self._generate_consensus_keys()
        self._create_nodes()
        self._init_consensus()

        event("simulator_setup_complete", node_id="system", view=0,
            consensus_type="my")
        
    def _generate_identities(self):
        self.identities, self.did_pub = generate_multiple_identities(max(50, self.num_nodes + 10))
    
    def _generate_consensus_keys(self):
        """生成共识密钥（简化版本）"""
        try:
            # 尝试调用BLS的密钥生成方法
            self.consensus_sks, self.consensus_pks, self.group_pk = BLS.generate_partial_keys(self.num_nodes)
        except AttributeError:
            logger.info(f"[Simulator] BLS.generate_partial_keys not found, using mock keys")
            # 使用模拟密钥
            self.consensus_sks = [f"sk_{i}".encode() for i in range(self.num_nodes)]
            self.consensus_pks = [f"pk_{i}".encode() for i in range(self.num_nodes)]
            self.group_pk = b"mock_group_pk"

    def _create_nodes(self):
        for i, nid in enumerate(self.node_ids):
            ident = self.identities[i]
            node = Node(
                node_id=nid,
                index=i,
                did_priv=ident["priv"],
                did_pub=ident["pub"],
                priv_key=self.consensus_sks[i],
                pub_key=self.consensus_pks[i],
                group_pk=self.group_pk,
                network=self.net,
                f=self.f,
                all_nodes=self.node_ids
            )
            # 注入映射和 metrics
            node.did_pub_lookup = {x["did"]: x["pub"] for x in self.identities}
            node.consensus_pub_lookup = {self.node_ids[j]: self.consensus_pks[j] for j in range(self.num_nodes)}
            node.metrics = self.metrics
            node.state.add_block(self.genesis_block)
            self.nodes[nid] = node
            self.net.register(node)
            event("node_created", node_id=nid, view=0, consensus_type="my")


    def _init_consensus(self):
        self.consensus = Consensus(self.nodes, self.net, f=self.f)
        self.consensus.group_pk = self.group_pk
        self.consensus.mempool = self.mempool
        self.consensus.max_rounds = 10
    
    # simulator.py - 修改相关部分
    def fill_mempool_registers(self, count=10):
        """填充内存池（注册交易）"""
        creators = self.identities
        ncre = len(creators)
        
        logger.info(f"[Simulator] 开始填充内存池，目标数量: {count}")
        
        successful_adds = 0
        for i in range(count):
            creator = creators[i % ncre]
            asset = self.assets[i % len(self.assets)]
            
            # 创建交易
            try:
                tx = create_register_tx(creator, asset)
                
                # 添加到内存池
                if self.mempool.push(tx):
                    successful_adds += 1
                    # 记录指标
                    tx_hash = tx_id(tx)
                    self.metrics.mark_created(tx_hash)
                    
                    if successful_adds % 100 == 0:
                        logger.info(f"[Simulator] 已添加 {successful_adds} 个交易到内存池")
                else:
                    logger.info(f"[Simulator] 交易重复或添加失败: {i}")
                    
            except Exception as e:
                logger.info(f"[Simulator] 创建交易失败: {e}")
                continue
        
        logger.info(f"[Simulator] 内存池填充完成，成功添加: {successful_adds}/{count}")
        logger.info(f"[Simulator] 内存池当前大小: {self.mempool.size()}")
        
        # event("mempool_fill_complete", node_id="system", view=0, 
        #       actual_count=count, consensus_type="my")
    
    def run(self, rounds=100, attack_type="none", attack_round=0) -> threading.Thread:
        """
        运行模拟器
        参数：
            rounds: 运行的总轮数
            attack_type: 攻击类型 ("none", "split_proposal", "view_number_attack", "fake_bitmap")
            attack_round: 发动攻击的轮数
        """
        self.max_rounds = rounds
        self.current_round = 0
        
        # 配置攻击
        self.attack_config = {
            "enabled": attack_type != "none",
            "attack_round": attack_round,
            "attack_type": attack_type
        }
        
        # 启动共识控制器
        if self.consensus:
            event("consensus_start", node_id="system", view=0, 
                 consensus_type="my")
            
            # 启动共识线程
            consensus_thread = self.consensus.start()
            
            # 同时启动模拟器监控线程
            sim_thread = threading.Thread(target=self._monitor_and_control, args=(rounds,), daemon=True)
            sim_thread.start()
            
            return consensus_thread
        else:
            raise RuntimeError("Consensus controller not initialized. Call setup() first.")
    
    def _monitor_and_control(self, rounds: int) -> None:
        """监控和控制模拟器运行（后台线程）"""
        start_time = time.time()
        last_stats_time = start_time
        stats_interval = 2.0  # 每2秒打印一次统计
        
        while self.current_round < rounds and self.consensus and self.consensus.active:
            # 检查是否到达攻击轮数
            # if (self.attack_config["enabled"] and 
            #     self.current_round >= self.attack_config["attack_round"]):
            #     # self._execute_attack()
            #     self.attack_config["enabled"] = False  # 只执行一次攻击
            
            # 定期打印统计信息
            current_time = time.time()
            if current_time - last_stats_time >= stats_interval:
                self._print_stats()
                last_stats_time = current_time
            
            # 更新当前轮数（从共识控制器获取）
            if hasattr(self.consensus, 'current_round'):
                self.current_round = self.consensus.current_round
            
            time.sleep(0.1)
        
        # 运行结束
        run_time = time.time() - start_time
        self._print_final_stats(run_time)
        
        # 停止共识
        if self.consensus:
            self.consensus.stop()
        
        event("simulation_complete", node_id="system", view=self.current_round,
              consensus_type="my")
    
    def _execute_attack(self) -> None:
        """执行攻击（根据配置的攻击类型）"""
        attack_type = self.attack_config["attack_type"]
        current_view = self.consensus.view if self.consensus else 0
        
        event("attack_triggered", node_id="system", view=current_view,
             round=self.current_round, consensus_type="my")
        
        if attack_type == "split_proposal":
            pass
            # self._split_proposal_attack(current_view)
        elif attack_type == "view_number_attack":
            pass
            # self._view_number_attack(current_view)
        elif attack_type == "fake_bitmap":
            pass
            # self._fake_bitmap_attack(current_view)
        else:
            pass
            event("attack_unknown", node_id="system", view=current_view,
                consensus_type="my")
    
    def _split_proposal_attack(self, current_view: int) -> None:
        """分裂提案攻击：恶意Leader向不同副本发送不同的提案"""
        # 获取当前Leader
        if not self.consensus:
            return
        
        leader_id = self.consensus.leader_for_view(current_view)
        leader = self.nodes.get(leader_id)
        
        if not leader or not leader.is_leader:
            return
        
        # 创建两个冲突的交易
        if not self.identities or not self.assets:
            return
        
        creator = self.identities[0]
        asset = self.assets[0]
        alice_did = creator["did"]
        alice_priv = creator["priv"]
        
        # 创建两个冲突的转移交易
        from transaction import create_transfer_tx

        # 创建 from_actor 字典
        alice_actor = {
            "did": alice_did,
            "priv": alice_priv  # 确保这是 sign_message 所需的私钥格式
        }
        
        # 交易A：转移到攻击者A
        tx_a = create_transfer_tx(
            from_actor=alice_actor, 
            to_did="did:sim:attackerA",
            image_hash=asset
        )

        # 交易B：转移到攻击者B（冲突）
        tx_b = create_transfer_tx(
            from_actor=alice_actor,  
            to_did="did:sim:attackerB",
            image_hash=asset
        )
        
        # 将节点分成两组
        nodes_list = list(self.nodes.keys())
        half = len(nodes_list) // 2
        group_a = nodes_list[:half]
        group_b = nodes_list[half:]
        
        # 创建两个冲突的区块
        parent_hash = leader.state.latest_qc.block_hash if leader.state.latest_qc else self.genesis_block.hash
        
        block_a = Block(
            parent_hash=parent_hash,
            height=self.current_round + 1,
            proposer=leader_id,
            payload=[tx_a],  # 只包含交易A
            qc=leader.state.latest_qc,
            view=current_view
        )
        
        block_b = Block(
            parent_hash=parent_hash,
            height=self.current_round + 1,
            proposer=leader_id,
            payload=[tx_b],  # 只包含交易B
            qc=leader.state.latest_qc,
            view=current_view
        )
        
        # 模拟恶意Leader向不同组发送不同提案
        # 注意：这绕过了正常的广播机制
        for nid in group_a:
            node = self.nodes[nid]
            proposal = {
                "block": block_a.to_dict() if hasattr(block_a, "to_dict") else block_a.__dict__,
                "qc": leader.state.latest_qc.to_dict() if leader.state.latest_qc and hasattr(leader.state.latest_qc, "to_dict") else None,
                "view": current_view,
                "transactions": [tx_a]
            }
            # 直接投递（模拟恶意行为）
            self.net.send_to(
                sender_id=leader_id,
                receiver_id=nid,
                message_type=self.net.MSG_TYPES["PROPOSAL"],
                message=proposal
            )
        
        for nid in group_b:
            node = self.nodes[nid]
            proposal = {
                "block": block_b.to_dict() if hasattr(block_b, "to_dict") else block_b.__dict__,
                "qc": leader.state.latest_qc.to_dict() if leader.state.latest_qc and hasattr(leader.state.latest_qc, "to_dict") else None,
                "view": current_view,
                "transactions": [tx_b]
            }
            self.net.send_to(
                sender_id=leader_id,
                receiver_id=nid,
                message_type=self.net.MSG_TYPES["PROPOSAL"],
                message=proposal
            )
        
        event("split_proposal_executed", node_id=leader_id, view=current_view,
             consensus_type="my")
    
    def _view_number_attack(self, current_view: int) -> None:
        """视图号攻击：恶意Leader尝试使用旧QC但修改view number"""
        if not self.consensus:
            return
        
        leader_id = self.consensus.leader_for_view(current_view)
        leader = self.nodes.get(leader_id)
        
        if not leader or not leader.state.latest_qc:
            return
        
        # 获取一个旧QC（例如前一个视图的QC）
        old_qc = leader.state.latest_qc
        
        # 创建伪造的QC，使用旧签名但新的view number
        fake_qc = QC(
            view=current_view + 10,  # 大幅增加view number
            block_hash=old_qc.block_hash,  # 使用旧区块哈希
            aggregate_signature=old_qc.aggregate_signature,  # 使用旧签名
            signer_bitmap=old_qc.signer_bitmap,  # 使用旧位图
            merkle_root=old_qc.merkle_root  # 使用旧Merkle根
        )
        
        # 创建包含伪造QC的提案
        parent_hash = old_qc.block_hash
        block = Block(
            parent_hash=parent_hash,
            height=self.current_round + 1,
            proposer=leader_id,
            payload=[],
            qc=fake_qc,  # 使用伪造的QC
            view=current_view
        )
        
        # 广播包含伪造QC的提案
        proposal = {
            "block": block,
            "qc": fake_qc,
            "view": current_view,
            "transactions": []
        }
        
        # 注意：这里我们实际上是在模拟恶意Leader的行为
        # 在真实场景中，恶意Leader会直接广播这个提案
        event("view_number_attack_executed", node_id=leader_id, view=current_view,
              consensus_type="my")
        
        # 记录攻击，用于后续验证
        self.recorded_attack = {
            "type": "view_number_attack",
            "old_qc": old_qc,
            "fake_qc": fake_qc,
            "block": block,
            "expected_to_fail": True  # 期望我们的安全机制能检测到这个攻击
        }
    
    def _fake_bitmap_attack(self, current_view: int) -> None:
        """伪造位图攻击：恶意Leader创建包含未签名副本的位图"""
        if not self.consensus:
            return
        
        leader_id = self.consensus.leader_for_view(current_view)
        leader = self.nodes.get(leader_id)
        
        if not leader or not leader.state.latest_qc:
            return
        
        # 创建包含未签名副本的位图
        fake_bitmap = 0
        all_indices = list(range(len(self.nodes)))
        
        # 只包含一半的副本（模拟恶意Leader排除某些副本）
        selected_indices = all_indices[:len(all_indices) // 2]
        for idx in selected_indices:
            fake_bitmap |= (1 << idx)
        
        # 添加一些未实际签名的副本（伪造）
        fake_indices = [idx for idx in all_indices if idx not in selected_indices][:2]
        for idx in fake_indices:
            fake_bitmap |= (1 << idx)  # 这些副本实际上没有签名
        
        # 创建伪造的QC
        fake_qc = QC(
            view=current_view,
            block_hash=leader.state.latest_qc.block_hash,
            aggregate_signature=leader.state.latest_qc.aggregate_signature,
            signer_bitmap=fake_bitmap,  # 伪造的位图
            merkle_root=leader.state.latest_qc.merkle_root
        )
        
        event("fake_bitmap_attack_executed", node_id=leader_id, view=current_view,
             consensus_type="my")
        
        # 记录攻击
        self.recorded_attack = {
            "type": "fake_bitmap_attack",
            "fake_bitmap": fake_bitmap,
            "real_indices": selected_indices,
            "fake_indices": fake_indices,
            "fake_qc": fake_qc,
            "expected_to_fail": True  # 期望我们的安全机制能检测到
        }
    
    def _print_stats(self) -> None:
        """打印当前统计信息"""
        if not self.consensus:
            return
        
        # 获取共识统计
        stats = self.consensus.get_stats() if hasattr(self.consensus, 'get_stats') else {}
        
        # 获取内存池统计
        mempool_size = len(self.mempool) if hasattr(self.mempool, '__len__') else 0
        
        # 获取提交的区块数
        committed_blocks = 0
        for node in self.nodes.values():
            if hasattr(node.state, 'commit_qc'):
                if node.state.commit_qc.view > 0:
                    committed_blocks += 1
        
        logger.info(f"\n=== 模拟器统计 (轮数: {self.current_round}/{self.max_rounds}) ===")
        logger.info(f"活跃节点: {len(self.nodes)}")
        logger.info(f"内存池大小: {mempool_size}")
        logger.info(f"已提交区块: {committed_blocks}")
        logger.info(f"视图: {self.consensus.view}")
        
        if stats:
            logger.info(f"提案数: {stats.get('proposals_made', 0)}")
            logger.info(f"成功QC数: {stats.get('qcs_formed', 0)}")
            logger.info(f"视图切换: {stats.get('view_changes', 0)}")
            logger.info(f"平均投票收集时间: {stats.get('avg_vote_collection_time', 0):.3f}s")
        
        # 检查是否有未处理的攻击
        if hasattr(self, 'recorded_attack') and self.recorded_attack:
            logger.info(f"⚠️  已记录攻击: {self.recorded_attack['type']}")
            logger.info(f"   预期检测: {'是' if self.recorded_attack.get('expected_to_fail', False) else '否'}")
    
    def _print_final_stats(self, run_time: float) -> None:
        """打印最终统计信息"""
        logger.info("\n" + "="*50)
        logger.info("模拟器运行完成!")
        logger.info("="*50)
        logger.info(f"总运行时间: {run_time:.2f}秒")
        logger.info(f"总轮数: {self.current_round}")
        logger.info(f"节点数: {len(self.nodes)}")
        logger.info(f"容错数 (f): {self.f}")
        
        # 计算TPS（粗略估计）
        total_transactions = 0
        for node in self.nodes.values():
            if hasattr(node, 'world_state'):
                # 假设world_state有交易计数
                pass
        
        if run_time > 0:
            logger.info(f"估算TPS: {total_transactions/run_time:.2f}")
        
        # 检查安全机制是否有效
        if hasattr(self, 'recorded_attack') and self.recorded_attack:
            logger.info(f"\n攻击测试结果:")
            logger.info(f"  攻击类型: {self.recorded_attack['type']}")
            logger.info(f"  预期被检测: {'是' if self.recorded_attack.get('expected_to_fail', False) else '否'}")
            
            # 这里可以添加实际的检测结果检查
            # 例如，检查是否有节点拒绝了伪造的QC
        
        logger.info("="*50)
    
    def stop(self) -> None:
        """停止模拟器"""
        self.active = False
        if self.consensus:
            self.consensus.stop()
        
        event("simulator_stopped", node_id="system", view=self.current_round, consensus_type="my")
    
    def verify_security_mechanisms(self) -> Dict[str, bool]:
        """
        验证安全机制是否有效
        返回：{安全机制名称: 是否通过测试}
        """
        results = {}
        
        # 1. 检查view number攻击检测
        results["view_number_attack_detection"] = self._test_view_number_attack_detection()
        
        # 2. 检查fake bitmap攻击检测
        results["fake_bitmap_detection"] = self._test_fake_bitmap_detection()
        
        # 3. 检查Merkle证明机制
        results["merkle_proof_mechanism"] = self._test_merkle_proof_mechanism()
        
        return results
    
    def _test_view_number_attack_detection(self) -> bool:
        """测试view number攻击检测机制"""
        # 简化测试：检查是否有节点记录了可疑QC
        for node in self.nodes.values():
            if hasattr(node.state, 'suspicious_qc_count') and node.state.suspicious_qc_count > 0:
                return True
        return False
    
    def _test_fake_bitmap_detection(self) -> bool:
        """测试fake bitmap攻击检测机制"""
        # 简化测试：检查是否有节点请求了Merkle证明
        for node in self.nodes.values():
            if hasattr(node, 'cached_proofs') and node.cached_proofs:
                return True
        return False
    
    def _test_merkle_proof_mechanism(self) -> bool:
        """测试Merkle证明机制"""
        # 检查网络是否支持证明请求/响应
        if not hasattr(self.net, 'MSG_TYPES'):
            return False
        
        required_msgs = ["REQUEST_MERKLE_PROOF", "MERKLE_PROOF"]
        for msg in required_msgs:
            if msg not in self.net.MSG_TYPES:
                return False
        
        return True

# 简化的运行示例
if __name__ == "__main__":
    # 创建模拟器
    simulator = Simulator(
        num_nodes=4,
        f=1,
        txs_per_proposal=10,
        drop_rate=0.05,
        delay_range=(0.01, 0.05)
    )
    
    # 设置模拟器
    simulator.setup()
    
    # 填充内存池
    simulator.fill_mempool_registers(count=1000)
    
    # 运行模拟器（可选：启用攻击测试）
    logger.info("启动模拟器...")
    thread = simulator.run(
        rounds=50,
        attack_type="view_number_attack",  # 测试view number攻击
        attack_round=10
    )
    
    # 等待模拟完成
    try:
        thread.join(timeout=30)  # 最多等待30秒
    except KeyboardInterrupt:
        logger.info("\n模拟被用户中断")
    finally:
        simulator.stop()
        
        # 验证安全机制
        results = simulator.verify_security_mechanisms()
        logger.info("\n安全机制验证结果:")
        for mechanism, passed in results.items():
            logger.info(f"  {mechanism}: {'✓ 通过' if passed else '✗ 失败'}")