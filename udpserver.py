import socket
import struct
import random
import time
from datetime import datetime
from collections import defaultdict

# 配置
BIND_IP = "172.18.203.113"
PORT = 9999
KEY = 0x5A3C
DROP_RATE = 0.1  # 30%丢包
GBN_WINDOW_SIZE = 3  # GBN窗口大小（服务端接收窗口）
TIMEOUT = 5  # 超时时间（秒，可选，客户端侧更关键）
FLUSH_TIMEOUT = 0.5  # 当缓冲非空且超过该秒数无新包时，强制刷新并发送ACK

# 客户端状态维护（GBN核心）
client_states = defaultdict(lambda: {
    "connected": False,
    "expected_seq": 0,
    # 缓存当前窗口内按序收到但尚未确认的数据
    "buffer": [],
    "last_active": 0,
    "last_ack": -1  # ==================== 新增：记录上一次发的ACK ====================
})

# ======================================
# 日志函数（自动写入 run_log.txt）
# ======================================
def write_log(msg):
    t = time.time()
    timestr = time.strftime("%Y-%m-%d %H:%M:%S")
    log = f"[{timestr}] {msg}"
    print(log)
    with open("run_log.txt", "a", encoding="utf-8") as f:
        f.write(log + "\n")

def get_server_time():
    return datetime.now().strftime("%H:%M:%S")

# 清理超时客户端连接
def clean_timeout_clients():
    now = time.time()
    for addr in list(client_states.keys()):
        if now - client_states[addr]["last_active"] > TIMEOUT * 2:
            del client_states[addr]
            write_log(f"[{addr}] 客户端超时，清理连接")


def flush_partial_buffers():
    """检查每个客户端的缓冲区：若缓冲区非空且自上次活动超过 FLUSH_TIMEOUT，则写入并发送累计ACK。"""
    now = time.time()
    for addr, state in client_states.items():
        if not state.get("connected"):
            continue
        if state.get("buffer") and (now - state.get("last_active", 0) > FLUSH_TIMEOUT):
            srv_time = get_server_time()
            try:
                with open("received_from_client.txt", "a", encoding="utf-8") as rf:
                    for s, m in state["buffer"]:
                        rf.write(m)
            except Exception as e:
                write_log(f"写入接收文件失败（flush）: {e}")

            ack_val = state["expected_seq"] - 1
            ack_msg = f"ACK|{ack_val}|{srv_time}"
            ack_bytes = ack_msg.encode("utf-8")
            sid = state.get("sid", 0)
            resp = struct.pack("!BHH", 1, sid, len(ack_bytes)) + ack_bytes
            try:
                srv.sendto(resp, addr)
                state["last_ack"] = ack_val
                write_log(f"[{addr}] 缓冲超时，强制刷新并发送累计ACK={ack_val}；写入 {len(state['buffer'])} 个包")
            except Exception as e:
                write_log(f"发送 ACK（flush）失败: {e}")

            state["buffer"].clear()

# 启动服务
srv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
srv.bind((BIND_IP, PORT))
srv.settimeout(1)
write_log(f"✅ GBN-UDP服务端启动，{BIND_IP}:{PORT} | 窗口大小={GBN_WINDOW_SIZE}")

while True:
    clean_timeout_clients()
    flush_partial_buffers()

    try:
        raw_data, addr = srv.recvfrom(2048)
    except socket.timeout:
        continue
    except Exception as e:
        write_log(f"接收数据异常: {e}")
        continue

    # 解析基础包头
    try:
        cmd, sid, dlen = struct.unpack("!BHH", raw_data[:5])
        payload = raw_data[5:5+dlen].decode("utf-8")
    except:
        write_log(f"[{addr}] 数据包解析失败，丢弃")
        srv.sendto(struct.pack("!BHH", 0xff, 0, 0), addr)
        continue

    # 验证学生ID
    real_stu = sid ^ KEY
    if not (0 <= real_stu <= 9999):
        srv.sendto(struct.pack("!BHH", 0xff, 0, 0), addr)
        write_log(f"[{addr}] 学生ID非法: {real_stu}")
        continue

    client_states[addr]["last_active"] = time.time()
    state = client_states[addr]

    # 1. 连接请求
    if cmd == 0:
        state["connected"] = True
        state["expected_seq"] = 0
        state["last_ack"] = -1
        state["sid"] = sid
        srv.sendto(struct.pack("!BHH", 0, sid, 0), addr)
        write_log(f"[{addr}] 学生 {real_stu} 连接成功 | 初始化GBN窗口")

    # 2. 业务数据（GBN 接收端核心逻辑）
    elif cmd == 1:
        if not state["connected"]:
            write_log(f"[{addr}] 未连接状态，丢弃数据")
            continue

        # 解析seq
        try:
            seq_str, msg = payload.split("|", 1)
            seq = int(seq_str)
        except:
            write_log(f"[{addr}] 数据格式错误，丢弃")
            continue

        srv_time = get_server_time()
        rand = random.random()

        # 丢包
        if rand < DROP_RATE:
            write_log(f"[{addr}] 模拟丢包，直接丢弃 | seq={seq} | 期望={state['expected_seq']}")
            continue

        # ==================== GBN 按序接收并缓冲到窗口满才ACK ====================
        if seq == state["expected_seq"]:
            state["buffer"].append((seq, msg))
            state["expected_seq"] += 1
            write_log(f"[{addr}] 缓存按序包 seq={seq}，当前缓冲大小={len(state['buffer'])}/{GBN_WINDOW_SIZE}")

            # 如果缓冲已满，批量写入文件并发送累计 ACK
            if len(state["buffer"]) >= GBN_WINDOW_SIZE:
                # 按序写入整窗数据
                try:
                    with open("received_from_client.txt", "a", encoding="utf-8") as rf:
                        for s, m in state["buffer"]:
                            rf.write(m)
                except Exception as e:
                    write_log(f"写入接收文件失败: {e}")

                # 发送累计ACK，ack 为已接收的最后序号
                ack_val = state["expected_seq"] - 1
                ack_msg = f"ACK|{ack_val}|{srv_time}"
                ack_bytes = ack_msg.encode("utf-8")
                resp = struct.pack("!BHH", 1, sid, len(ack_bytes)) + ack_bytes
                try:
                    srv.sendto(resp, addr)
                    state["last_ack"] = ack_val
                    write_log(f"[{addr}] 窗口已满，发送累计ACK={ack_val}；已将 {len(state['buffer'])} 个包写入文件")
                except Exception as e:
                    write_log(f"发送 ACK 失败: {e}")

                # 清空缓冲，为下一窗口准备
                state["buffer"].clear()

        else:
            # 处理重复包或乱序包，GBN 接收端不缓存乱序包
            if seq < state["expected_seq"]:
                write_log(f"[{addr}] 收到重复包 seq={seq}（已接收或已缓冲）")
            else:
                write_log(f"[{addr}] 收到乱序包 seq={seq}（期望 {state['expected_seq']}），丢弃")

            # 不满足整窗确认时，不主动发送新的 ACK（保持攒包策略）

    # 3. 终止/结束传输（cmd=2）：客户端表明没有更多数据，立即刷新并发送ACK
    elif cmd == 2:
        if not state["connected"]:
            write_log(f"[{addr}] 未连接的 EOT，丢弃")
            continue

        write_log(f"[{addr}] 收到客户端 EOT（无更多数据）请求刷新")
        # 立即写入缓冲并发送 ACK
        srv_time = get_server_time()
        if state.get("buffer"):
            try:
                with open("received_from_client.txt", "a", encoding="utf-8") as rf:
                    for s, m in state["buffer"]:
                        rf.write(m)
            except Exception as e:
                write_log(f"写入接收文件失败（EOT）: {e}")

            state["buffer"].clear()

        ack_val = state["expected_seq"] - 1
        ack_msg = f"ACK|{ack_val}|{srv_time}"
        ack_bytes = ack_msg.encode("utf-8")
        resp = struct.pack("!BHH", 1, sid, len(ack_bytes)) + ack_bytes
        try:
            srv.sendto(resp, addr)
            state["last_ack"] = ack_val
            write_log(f"[{addr}] 已处理 EOT，发送最终 ACK={ack_val}")
        except Exception as e:
            write_log(f"发送 EOT ACK 失败: {e}")

    # 其他未知命令
    else:
        srv.sendto(struct.pack("!BHH", 0xff, 0, 0), addr)

srv.close()