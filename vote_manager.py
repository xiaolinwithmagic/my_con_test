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
        network.broadcast_vote(
            sender_id=node_id,
            block_id=block.id,
            view=vote.view,
            partial_sig=vote.partial_signature,
            voter_index=vote.voter_index,
            block_hash=block.hash
        )