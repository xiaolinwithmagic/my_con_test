class Pacemaker:
    """简化的 Pacemaker 实现"""
    
    def __init__(self, node_id, f, network):
        self.node_id = node_id
        self.f = f
        self.network = network
        
        # 超时配置
        self.base_timeout = 2.0  # 基础超时
        self.timeout_multiplier = 1.5  # 超时乘数
        self.current_timeout = self.base_timeout
        
        # 心跳机制
        self.heartbeat_interval = 1.0
        self.last_heartbeat = 0
        self.heartbeat_missed = 0
        
        # 视图管理
        self.view = 0
        self.leader_timeouts = {}  # leader_id -> timeout_count
        
        # 监控
        self.round_trip_times = []  # RTT记录
        self.max_rtt_history = 10
        
    def adjust_timeout(self, success: bool):
        """根据成功/失败调整超时时间"""
        if success:
            # 成功：减少超时（但不能低于基础值）
            self.current_timeout = max(
                self.base_timeout,
                self.current_timeout / self.timeout_multiplier
            )
            logger.info(f"[Pacemaker] 减少超时到 {self.current_timeout:.2f}s")
        else:
            # 失败：增加超时
            self.current_timeout *= self.timeout_multiplier
            logger.info(f"[Pacemaker] 增加超时到 {self.current_timeout:.2f}s")
    
    def record_leader_performance(self, leader_id: str, success: bool):
        """记录领导者性能"""
        if leader_id not in self.leader_timeouts:
            self.leader_timeouts[leader_id] = {"success": 0, "timeout": 0}
        
        if success:
            self.leader_timeouts[leader_id]["success"] += 1
        else:
            self.leader_timeouts[leader_id]["timeout"] += 1
    
    def should_skip_leader(self, leader_id: str) -> bool:
        """判断是否应该跳过这个领导者"""
        if leader_id not in self.leader_timeouts:
            return False
        
        stats = self.leader_timeouts[leader_id]
        total = stats["success"] + stats["timeout"]
        
        if total < 3:  # 样本太少
            return False
        
        # 如果超时率超过50%，考虑跳过
        timeout_rate = stats["timeout"] / total
        return timeout_rate > 0.5
    
    def send_heartbeat_if_leader(self, node):
        """如果是领导者，发送心跳"""
        if node.is_leader and time.time() - self.last_heartbeat > self.heartbeat_interval:
            self.network.broadcast_heartbeat(
                sender_id=node.id,
                view=self.view,
                latest_qc=node.state.latest_qc
            )
            self.last_heartbeat = time.time()
    
    def check_heartbeat(self, leader_id: str, last_seen: float) -> bool:
        """检查心跳是否超时"""
        elapsed = time.time() - last_seen
        if elapsed > self.current_timeout * 2:  # 两倍超时时间
            self.heartbeat_missed += 1
            return False
        return True