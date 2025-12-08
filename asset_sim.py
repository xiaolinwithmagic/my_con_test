# asset_sim.py
import hashlib
import os
import random

def get_image_hash(image_path: str) -> str:
    """计算图片的 sha256（hex）"""
    sha = hashlib.sha256()
    with open(image_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha.update(chunk)
    return sha.hexdigest()

def generate_image_hashes_from_folder(folder_path: str, limit: int = 1000):
    """遍历文件夹，返回不超过 limit 的图片哈希列表"""
    hashes = []
    exts = (".jpg", ".jpeg", ".png", ".bmp", ".gif", ".tif", ".tiff")
    files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.lower().endswith(exts)]
    files = files[:limit]
    for p in files:
        try:
            hashes.append(get_image_hash(p))
        except Exception as e:
            # 忽略读图错误
            continue
    return hashes

def generate_dummy_hashes(count: int):
    """当没有真实图片时：生成伪造但唯一的哈希（用于实验）"""
    out = []
    for i in range(count):
        out.append(hashlib.sha256(f"dummy-image-{i}".encode()).hexdigest())
    return out
