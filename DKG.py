# dkg.py
"""
简易版 DKG（Shamir Secret Sharing）
用于模拟分布式密钥生成，在 ReVer-QC 中用于验证 “节点是否属于同一共享的组密钥”。
"""

from secrets import randbelow
from typing import List, Tuple


MODULUS = 2**256  # 简化：大素数域


class DKG:
    def __init__(self, threshold: int, num_nodes: int):
        self.threshold = threshold
        self.num_nodes = num_nodes

        # 多项式系数: a0 = secret
        self.secret = randbelow(MODULUS)
        self.coefficients = [self.secret] + [randbelow(MODULUS) for _ in range(threshold - 1)]

    def generate_shares(self) -> List[Tuple[int, int]]:
        """为每个节点生成 (index, share)"""
        shares = []
        for i in range(1, self.num_nodes + 1):
            y = 0
            for j, coeff in enumerate(self.coefficients):
                y = (y + coeff * pow(i, j, MODULUS)) % MODULUS
            shares.append((i, y))
        return shares

    @staticmethod
    def reconstruct(shares: List[Tuple[int, int]], threshold: int) -> int:
        """使用 Lagrange 插值重建秘密"""

        def lagrange_interpolate(x, xs, ys):
            total = 0
            for i in range(len(xs)):
                xi, yi = xs[i], ys[i]
                li = 1
                for j in range(len(xs)):
                    if i != j:
                        xj = xs[j]
                        inv = pow(xi - xj, MODULUS - 2, MODULUS)
                        li = (li * (x - xj) * inv) % MODULUS
                total = (total + yi * li) % MODULUS
            return total

        x_s, y_s = zip(*shares)
        return lagrange_interpolate(0, x_s, y_s)
