# 共识参数
NUM_NODES = 4
FAULTS = (NUM_NODES - 1) // 3  # 动态计算 f
TIMEOUT_MS = 200  # 超时时间（毫秒）
MAX_BLOCK_SIZE = 100  # 每个区块的最大交易数量
MAX_RETRIES = 3  # 消息重试次数

# BLS参数
SUPPORTED_CURVES = ["BLS12-381", "BN254"]
BLS_CURVE = "BLS12-381"
if BLS_CURVE not in SUPPORTED_CURVES:
    raise ValueError(f"Unsupported BLS curve: {BLS_CURVE}")

# 日志
ENABLE_LOG = True
LOG_LEVEL = "DEBUG"  # 可选值: DEBUG, INFO, WARNING, ERROR
LOG_FILE = "hotstuff.log"

# 环境配置
ENV = "TEST"  # 可选值: TEST, PROD
if ENV == "TEST":
    TIMEOUT_MS = 100
elif ENV == "PROD":
    TIMEOUT_MS = 500