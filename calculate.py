import json
from collections import defaultdict

def parse_consensus_metrics(log_file: str = "trace1.log"):
    """解析共识埋点日志（支持区分PBFT/HotStuff），计算时延、投票、视图切换、分叉等指标"""
    # ===================== 核心修改1：新增consensus_type维度的事件分组 =====================
    # 结构：block_events[consensus_type][(block_id, view)][event_type] = [事件字典]
    block_events = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    all_events = []  # 存储所有事件，用于统计视图切换/分叉/轮换

    with open(log_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
                all_events.append(event)
                # 提取共识类型（默认unknown）
                c_type = event.get("consensus_type", "unknown")
                # 按block_id+view分组（必须同时有这两个字段才分组）
                if "block_id" in event and "view" in event:
                    key = (event["block_id"], event["view"])
                    block_events[c_type][key][event["event"]].append(event)
            except json.JSONDecodeError:
                print(f"跳过无效JSON行：{line}")
                continue

    # ===================== 核心修改2：按consensus_type初始化指标存储 =====================
    metrics = defaultdict(lambda: {
        "latency": {  # 时延指标（按block_id+view+node）
            "proposal_broadcast_to_received": [],  # 每个节点的广播→接收时延
            "received_to_vote_sent": [],           # 每个节点的接收→投票时延
            "vote_to_qc_built": [],                # 每个区块的投票→QC时延
            "qc_to_block_committed": [],           # 每个节点的QC→提交时延
            "broadcast_to_committed": []           # 每个节点的广播→提交总时延
        },
        "vote": {  # 投票指标
            "block_voter_count": {},               # (block_id, view): 投票节点数
            "vote_type_dist": defaultdict(int)     # vote_type: 次数
        },
        "view_change": {  # 视图切换指标（日志中暂无则为空）
            "trigger_count": 0,
            "reason_dist": defaultdict(int)
        },
        "fork": {  # 分叉指标（日志中暂无则为空）
            "detect_count": 0,
            "fork_chain_length_dist": defaultdict(int),
            "broadcast_to_fork_latency": []
        },
        "rotation": {  # 视图轮换指标（日志中暂无则为空）
            "rotation_duration": []
        }
    })

    # ===================== 3. 按共识类型遍历计算核心指标 =====================
    for c_type, type_block_events in block_events.items():
        print(f"\n==================== 共识类型：{c_type.upper()} ====================")
        
        # 3.1 计算该类型下的时延和投票指标
        for (block_id, view), event_map in type_block_events.items():
            # 3.1.1 提案广播→接收时延（需先获取该区块的广播事件）
            broadcast_events = event_map.get("proposal_broadcast", [])
            if not broadcast_events:
                continue
            broadcast_ts = broadcast_events[0]["ts"]  # 一个区块只有一次广播

            # 3.1.2 遍历每个节点的接收/投票/提交事件
            received_events = event_map.get("proposal_received", [])
            for recv_event in received_events:
                node = recv_event["node"]
                recv_ts = recv_event["ts"]
                # 广播→接收时延
                br_latency = recv_ts - broadcast_ts
                metrics[c_type]["latency"]["proposal_broadcast_to_received"].append({
                    "block_id": block_id,
                    "view": view,
                    "node": node,
                    "latency": round(br_latency, 6)
                })

                # 接收→投票时延（找该节点的vote_sent事件）
                vote_events = [v for v in event_map.get("vote_sent", []) if v["node"] == node]
                if vote_events:
                    vote_ts = vote_events[0]["ts"]
                    rv_latency = vote_ts - recv_ts
                    metrics[c_type]["latency"]["received_to_vote_sent"].append({
                        "block_id": block_id,
                        "view": view,
                        "node": node,
                        "latency": round(rv_latency, 6)
                    })
                    # 统计投票类型
                    vote_type = vote_events[0].get("vote_type", "unknown")
                    metrics[c_type]["vote"]["vote_type_dist"][vote_type] += 1

            # 3.1.3 投票→QC构建时延（找该区块最后一个投票的ts）
            vote_events = event_map.get("vote_sent", [])
            qc_events = event_map.get("qc_built", [])
            if vote_events and qc_events:
                last_vote_ts = max([v["ts"] for v in vote_events])
                qc_ts = qc_events[0]["ts"]  # 一个区块的QC构建取第一个（多轮QC取首次）
                vq_latency = qc_ts - last_vote_ts
                metrics[c_type]["latency"]["vote_to_qc_built"].append({
                    "block_id": block_id,
                    "view": view,
                    "latency": round(vq_latency, 6)
                })

                # 3.1.4 QC→区块提交时延（遍历每个节点的提交事件）
                commit_events = event_map.get("block_committed", [])
                for commit_event in commit_events:
                    node = commit_event["node"]
                    commit_ts = commit_event["ts"]
                    qc_commit_latency = commit_ts - qc_ts
                    metrics[c_type]["latency"]["qc_to_block_committed"].append({
                        "block_id": block_id,
                        "view": view,
                        "node": node,
                        "latency": round(qc_commit_latency, 6)
                    })
                    # 广播→提交总时延
                    bc_latency = commit_ts - broadcast_ts
                    metrics[c_type]["latency"]["broadcast_to_committed"].append({
                        "block_id": block_id,
                        "view": view,
                        "node": node,
                        "latency": round(bc_latency, 6)
                    })

            # 3.1.5 统计单个区块的投票节点数
            voter_nodes = {v["node"] for v in event_map.get("vote_sent", [])}
            metrics[c_type]["vote"]["block_voter_count"][(block_id, view)] = len(voter_nodes)

        # ===================== 4. 按共识类型统计视图切换指标 =====================
        view_change_events = [e for e in all_events if e.get("consensus_type") == c_type and e["event"] == "view_change"]
        metrics[c_type]["view_change"]["trigger_count"] = len(view_change_events)
        for vc_event in view_change_events:
            reason = vc_event.get("reason", "unknown")
            metrics[c_type]["view_change"]["reason_dist"][reason] += 1

        # ===================== 5. 按共识类型统计分叉指标 =====================
        fork_events = [e for e in all_events if e.get("consensus_type") == c_type and e["event"] == "fork_detected"]
        metrics[c_type]["fork"]["detect_count"] = len(fork_events)
        for fork_event in fork_events:
            block_id = fork_event["block_id"]
            view = fork_event["view"]
            fork_ts = fork_event["ts"]
            # 找对应区块的广播ts
            broadcast_events = [e for e in all_events if e.get("consensus_type") == c_type 
                                                  and e["event"] == "proposal_broadcast" 
                                                  and e["block_id"] == block_id 
                                                  and e["view"] == view]
            if broadcast_events:
                bf_latency = fork_ts - broadcast_events[0]["ts"]
                metrics[c_type]["fork"]["broadcast_to_fork_latency"].append({
                    "block_id": block_id,
                    "view": view,
                    "latency": round(bf_latency, 6)
                })
            # 统计分叉链长度
            chain_len = fork_event.get("fork_chain_length", 0)
            metrics[c_type]["fork"]["fork_chain_length_dist"][chain_len] += 1

        # ===================== 6. 按共识类型统计视图轮换指标 =====================
        rotation_start = {e["view"]: e["ts"] for e in all_events if e.get("consensus_type") == c_type and e["event"] == "did_rotation_start"}
        rotation_end = {e["view"]: e["ts"] for e in all_events if e.get("consensus_type") == c_type and e["event"] == "did_rotation_end"}
        for view, start_ts in rotation_start.items():
            if view in rotation_end:
                duration = rotation_end[view] - start_ts
                metrics[c_type]["rotation"]["rotation_duration"].append({
                    "view": view,
                    "duration": round(duration, 6)
                })

        # ===================== 7. 按共识类型输出结构化结果 =====================
        print(f"\n===== {c_type.upper()} - 1. 时延指标（单位：秒） =====")
        for latency_type, data in metrics[c_type]["latency"].items():
            if not data:
                print(f"{latency_type}: 无数据")
                continue
            print(f"\n{latency_type}:")
            # 计算平均值
            avg_latency = round(sum([d["latency"] for d in data]) / len(data), 6)
            print(f"  平均值: {avg_latency}")
            # 输出前3条明细（示例）
            for i, d in enumerate(data[:3]):
                print(f"  示例{i+1}: {d}")

        print(f"\n===== {c_type.upper()} - 2. 投票指标 =====")
        print(f"各区块投票节点数: {metrics[c_type]['vote']['block_voter_count']}")
        print(f"投票类型分布: {dict(metrics[c_type]['vote']['vote_type_dist'])}")

        print(f"\n===== {c_type.upper()} - 3. 视图切换指标 =====")
        print(f"视图切换触发次数: {metrics[c_type]['view_change']['trigger_count']}")
        print(f"视图切换原因分布: {dict(metrics[c_type]['view_change']['reason_dist'])}")

        print(f"\n===== {c_type.upper()} - 4. 分叉指标 =====")
        print(f"分叉检测次数: {metrics[c_type]['fork']['detect_count']}")
        print(f"分叉链长度分布: {dict(metrics[c_type]['fork']['fork_chain_length_dist'])}")
        print(f"提案广播→分叉检测时延: {metrics[c_type]['fork']['broadcast_to_fork_latency']}")

        print(f"\n===== {c_type.upper()} - 5. 视图轮换指标 =====")
        print(f"视图轮换耗时: {metrics[c_type]['rotation']['rotation_duration']}")

    return metrics

if __name__ == "__main__":
    # 运行解析（支持区分PBFT/HotStuff）
    parse_consensus_metrics("trace1.log")