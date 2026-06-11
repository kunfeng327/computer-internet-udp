import socket
import struct
import random
import time
import signal
from datetime import datetime
from collections import defaultdict

# 配置
BIND_IP = "172.18.203.113"
PORT = 9999
KEY = 0x5A3C
DROP_RATE = 0.1
GBN_WINDOW_SIZE = 5

TIMEOUT = 1
FLUSH_TIMEOUT = 0.5
running = True

# ===================== Ctrl+C 退出处理 =====================
def handle_exit(signum, frame):
    global running
    print("\n🛑 收到 Ctrl+C，服务器正在安全关闭...")
    running = False
    
signal.signal(signal.SIGINT, handle_exit)
# ==========================================================

client_states = defaultdict(lambda: {
    "connected": False,
    "expected_seq": 0,
    "buffer": [],
    "last_active": 0,
    "last_ack": -1,
    "sid": 0,
    "client_rcv": 0,
    "client_drop": 0
})

def write_log(msg):
    timestr = time.strftime("%Y-%m-%d %H:%M:%S.") + f"{int(time.time() * 1000) % 1000:03d}"
    log = f"[{timestr}] {msg}"
    print(log)
    with open("run_log.txt", "a", encoding="utf-8") as f:
        f.write(log + "\n")

def get_server_time():
    return datetime.now().strftime("%H:%M:%S.") + f"{datetime.now().microsecond // 1000:03d}"

def clean_timeout_clients():
    now = time.time()
    for addr in list(client_states.keys()):
        state = client_states[addr]
        if now - state["last_active"] > TIMEOUT * 2:
            del client_states[addr]
            write_log(f"[{addr}] 客户端超时，清理连接")
            total_processed = state["client_rcv"] + state["client_drop"]
            if total_processed > 0:
                drop_rate_real = (state["client_drop"] / total_processed) * 100
            else:
                drop_rate_real = 0.0
            print(f"\n📊 统计结果：总接收={state['client_rcv']}，总丢包={state['client_drop']}，丢包率={drop_rate_real:.2f}%")

def flush_partial_buffers():
    now = time.time()
    for addr, state in client_states.items():
        if not state.get("connected"): continue
        if state.get("buffer") and (now - state.get("last_active", 0) > FLUSH_TIMEOUT):
            try:
                with open("received_from_client.txt", "ab") as rf:
                    for s, m in state["buffer"]:
                        rf.write(m)
            except Exception as e:
                write_log(f"写入失败（flush）: {e}")

            ack_val = state["expected_seq"] - 1
            # ===================== 7B ACK 包 =====================
            resp = struct.pack("!BHHh", 1, state["sid"], 0, ack_val)
            try:
                srv.sendto(resp, addr)
                state["last_ack"] = ack_val
            except: pass
            state["buffer"].clear()

# 启动服务
srv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
srv.bind((BIND_IP, PORT))
srv.settimeout(1)
write_log(f"✅ GBN服务端启动 {BIND_IP}:{PORT} | 窗口={GBN_WINDOW_SIZE}")

while running:
    clean_timeout_clients()
    flush_partial_buffers()

    try:
        raw_data, addr = srv.recvfrom(2048)
    except socket.timeout:
        continue
    except:
        continue

    # ===================== 解包 7B 首部 !BHHH =====================
    try:
        cmd, sid, dlen, seq = struct.unpack("!BHHH", raw_data[:7])
        payload = raw_data[7:7+dlen]
    except:
        continue

    real_stu = sid ^ KEY
    if not (0 <= real_stu <= 9999): continue

    client_states[addr]["last_active"] = time.time()
    state = client_states[addr]

    # 连接 cmd=0
    if cmd == 0:
        state["connected"] = True
        state["expected_seq"] = 0
        state["last_ack"] = -1
        state["sid"] = sid
        # ===================== 7B 回复 =====================
        srv.sendto(struct.pack("!BHHH", 0, sid, 0, 0), addr)
        write_log(f"[{addr}] 学生 {real_stu} 连接成功")

    # 数据报文 cmd=1
    elif cmd == 1:
        if not state["connected"]: continue

        # 客户端已删除 seq| ，直接使用首部中的 seq
        seq_num = seq

        # 模拟丢包
        if random.random() < DROP_RATE:
            write_log(f"[{addr}] 丢包 seq={seq_num}")
            state["client_drop"] += 1
            continue

        # GBN 按序接收
        if seq_num == state["expected_seq"]:
            state["buffer"].append((seq_num, payload))
            state["expected_seq"] += 1
            write_log(f"[{addr}] 接收 seq={seq_num}")
            state["client_rcv"] += 1
            state["last_ack"] = state["expected_seq"] - 1

            if len(state["buffer"]) >= GBN_WINDOW_SIZE:
                with open("received_from_client.txt", "ab") as f:
                    for s, m in state["buffer"]:
                        f.write(m)
                state["buffer"].clear()

        # 回复 ACK（7B）
        last_ack = state.get("last_ack", -1)
        if seq_num > state["expected_seq"]:
            write_log(f"[{addr}] 乱序包 seq={seq_num}，丢弃，回复 ACK={last_ack}")
        # ===================== 7B ACK =====================
        resp = struct.pack("!BHHh", 1, sid, 0, last_ack)
        srv.sendto(resp, addr)

    # EOT 结束 cmd=2
    elif cmd == 2:
        if not state["connected"]: continue
        write_log(f"[{addr}] 收到 EOT")

        if state["buffer"]:
            with open("received_from_client.txt", "ab") as f:
                for s, m in state["buffer"]:
                    f.write(m)
            state["buffer"].clear()

        # 最终 ACK
        ack_val = state["expected_seq"] - 1
        # ===================== 7B =====================
        resp = struct.pack("!BHHh", 1, sid, 0, ack_val)
        srv.sendto(resp, addr)
        state["last_ack"] = ack_val

srv.close()
write_log("✅ 服务器已正常关闭")
