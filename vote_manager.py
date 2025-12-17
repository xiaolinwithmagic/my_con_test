import logging
from typing import Optional
from block import Block
from network import Network
from votes import Vote


logger = logging.getLogger(__name__)



class VoteManager:
    """统一的投票管理器"""
    
    @staticmethod
    def create_vote(node: any, block: Block, view: int) -> Optional[Vote]:
        """创建投票 - 统一实现"""
        try:
             # 参数处理
            if view is None:
                view = node.view
            
            # 验证参数
            if not node or not block:
                logger.error("创建投票失败: 节点或区块为空")
                return None

            # 检查私钥
            if not hasattr(node, 'priv') or node.priv is None:
                logger.error(f"节点 {node.id} 无BLS私钥")
                return None
            
            # 准备签名数据
            try:
                logger.debug(f" 准备签名数据: view={view}, block_hash={block.hash}")
                block_hash = block.hash
                if isinstance(block_hash, str):
                    block_hash_bytes = block_hash.encode('utf-8')
                else:
                    block_hash_bytes = block_hash
                logger.debug(f" block_hash_bytes 长度: {len(block_hash_bytes)}")
                view_bytes = view.to_bytes(8, 'big')
                # 拼接签名数据: view || block_hash
                sign_data = view_bytes + block_hash_bytes
            except Exception as e:
                logger.error(f"准备签名数据失败: {e}")
                return None
            
            try:
                # logger.debug(f"view_bytes: {view_bytes.hex()}, 长度: {len(view_bytes)}")
                # logger.debug(f"block_hash: {block_hash.hex()}, 长度: {len(block_hash)}")
                # logger.debug(f"sign_data: {sign_data.hex()}, 长度: {len(sign_data)}")

                # 签名
                from crypto import BLS
                partial_sig = BLS.sign(node.priv, sign_data)
                logger.debug(f" partial_sig 长度: {len(partial_sig)}")
            except Exception as e:
                logger.error(f"BLS签名失败 (节点={node.id}): {e}")
                return None

            # 创建投票对象
            vote = Vote(
                voter_id=node.id,
                voter_index=getattr(node, 'index', 0),
                block_hash=block.hash,
                view=view,
                partial_signature=partial_sig,
                high_qc=getattr(node.state, 'latest_qc', None)
            )
            logger.info(f" {node.id} 创建投票成功")
            return vote
        except Exception as e:
            logger.error(f"创建投票失败: {e}", exc_info=True)
            return None
    
    @staticmethod
    def broadcast_vote(network: Network, node_id: str, vote: Vote, block: Block):
        """广播投票"""
        try:
            logger.info(f"[VoteManager] 节点 {node_id} 开始广播投票")
            logger.info(f"[VoteManager] network实例: {network}, 类型: {type(network)}")
            logger.info(f"[VoteManager] vote对象: {vote}, block对象: {block}")
            
            # 检查参数
            if network is None:
                logger.error(f"[VoteManager] network实例为None")
                return False
                
            if vote is None:
                logger.error(f"[VoteManager] vote对象为None")
                return False
                
            if block is None:
                logger.error(f"[VoteManager] block对象为None")
                return False
            
            logger.info(f"[VoteManager] 调用network.broadcast_vote, 参数: node_id={node_id}, vote.view={vote.view}")
            
            # 调用network的广播方法
            # result = network.broadcast_vote(
            #     sender_id=node_id,
            #     # block_id=block.id,
            #     view=vote.view,
            #     partial_sig=vote.partial_signature,
            #     voter_index=vote.voter_index,
            #     block_hash=block.hash
            # )
            result = network.broadcast_vote(
                sender_id=node_id,
                vote=vote,  # 传递整个vote对象
                block=block,  # 传递整个block对象
            )
            # 2025-12-15 20:21:35,362 - INFO - [VoteManager] vote对象: Vote(id=0afe7e5946cb4198, voter=node0, index=0, view=1), block对象: Block(hash=a84f2ccf, height=1, proposer=node1, view=1)

                # 添加额外信息
            # message = {
            #     "vote": vote_dict,  # 序列化后的投票
            #     "block": {
            #         "id": block.id,
            #         "hash": block.hash,
            #         "height": block.height if hasattr(block, 'height') else None,
            #         "proposer": block.proposer if hasattr(block, 'proposer') else None,
            #         "transactions": block.transactions if hasattr(block, 'transactions') else [],
            #         "timestamp": block.timestamp if hasattr(block, 'timestamp') else time.time()
            #     },
            #     "timestamp": time.time()
            # }
            if result is None:
                logger.info(f"[VoteManager] 投票广播完成，结果: ")

            logger.info(f"[VoteManager] 投票广播完成，结果: {result}")
            return result
            
        except Exception as e:
            logger.error(f"[VoteManager] 广播投票异常: {e}", exc_info=True)
            return False
        