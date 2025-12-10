# dkg_bls.py
from crypto import BLS

class DKG:
    """
    简化版 BLS 阈值密钥生成
    （目前不做 Shamir，而是模拟所有节点参与生成 group key）
    """

    def __init__(self, num_nodes):
        self.num_nodes = num_nodes
        self.sks, self.pks, self.group_pk = BLS.generate_partial_keys(num_nodes)

    def get_partial_key(self, index):
        return self.sks[index], self.pks[index]

    def get_group_public_key(self):
        return self.group_pk
