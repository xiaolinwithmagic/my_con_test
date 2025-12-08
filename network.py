# network.py
import random
import time
import logging
from threading import Thread

logging.basicConfig(level=logging.DEBUG)

class Network:
    def __init__(self, drop_rate=0.1, delay_range=(0.05, 0.2)):
        self.nodes = {}
        self.drop_rate = drop_rate
        self.delay_range = delay_range

    def register(self, node):
        self.nodes[node.id] = node
        logging.debug(f"Node {node.id} registered to the network.")

    def unregister(self, node_id):
        if node_id in self.nodes:
            del self.nodes[node_id]
            logging.debug(f"Node {node_id} unregistered from the network.")

    def _deliver(self, receiver, message_type, message):
        if random.random() > self.drop_rate:
            delay = random.uniform(*self.delay_range)
            time.sleep(delay)
            logging.debug(f"Delivering {message_type} to {receiver.id} after {delay:.2f}s delay.")
            receiver.receive_message(message_type, message)
        else:
            logging.warning(f"Message {message_type} lost to {receiver.id}.")

    def broadcast(self, sender_id, message_type, message):
        logging.debug(f"[Broadcast] {message_type} from {sender_id}: {message}")
        for nid, node in list(self.nodes.items()):
            if nid != sender_id:
                Thread(target=self._deliver, args=(node, message_type, message)).start()

    def broadcast_vote(self, sender_id, vote):
        self.broadcast(sender_id, "vote", vote)

    def broadcast_soft_vote(self, sender_id, block_id):
        self.broadcast(sender_id, "soft_vote", {"block_id": block_id, "sender": sender_id})

    def broadcast_partial_signature(self, sender_id, partial_sig):
        self.broadcast(sender_id, "partial_signature", partial_sig)

    def broadcast_partial_qc(self, sender_id, partial_certificate):
        self.broadcast(sender_id, "partial_qc", partial_certificate)

    def broadcast_view_change(self, sender_id, vc_message):
        self.broadcast(sender_id, "view_change", vc_message)
