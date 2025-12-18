import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor

class AsyncNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.loop = asyncio.get_event_loop()
        self.executor = ThreadPoolExecutor(max_workers=10)



import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor

class AsyncNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.loop = asyncio.get_event_loop()
        self.executor = ThreadPoolExecutor(max_workers=10)
        
    # async def handle_message_async(self, msg_type, msg_data):
    #     """异步处理消息"""
    #     try:
    #         if msg_type == "proposal":
    #             # 提案处理可能涉及CPU密集型操作，放到线程池
    #             result = await self.loop.run_in_executor(
    #                 self.executor, 
    #                 self._handle_proposal_sync, 
    #                 msg_data
    #             )
    #         elif msg_type == "vote":
    #             result = await self.loop.run_in_executor(
    #                 self.executor,
    #                 self._handle_vote_sync,
    #                 msg_data
    #             )
    #         else:
    #             # 其他消息直接处理
    #             result = await self._handle_other_message(msg_data)
            
    #         return result
    #     except Exception as e:
    #         logger.error(f"异步处理消息失败: {e}")
    #         return False