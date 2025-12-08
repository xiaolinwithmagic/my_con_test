# fingerprint.py
import hashlib
import os

def get_image_hash(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()

def load_image_hashes(folder):
    hashes = []
    for f in os.listdir(folder):
        fp = os.path.join(folder, f)
        if os.path.isfile(fp):
            hashes.append(get_image_hash(fp))
    return hashes
