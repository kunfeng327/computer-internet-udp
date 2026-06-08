import socket
import struct
import random
import time
import signal  # 新增
from datetime import datetime
from collections import defaultdict

# 配置
BIND_IP = "172.18.203.113"
PORT = 9999
KEY = 0x5A3C
DROP_RATE = 0.1  # 0=不丢包，方便测试
GBN_WINDOW_SIZE = 5
TIMEOUT = 5
FLUSH_TIMEOUT = 0.5
running = True

# ===================== Ctrl+C 退出处理 =====================
def handle_exit(signum, frame):
    global running
    print("\n🛑 收到 Ctrl+C，服务器正在安全关闭...")
    running = False

# 注册信号
signal.signal(signal.SIGINT, handle_exit)
# ==========================================================

client_states = defaultdict(lambda: {
    "connected": False,
    "expected_seq": 0,
    "buffer": [],
    "last_active": 0,
    "last_ack": -1,
    "sid": 0
})

def write_log(msg):
    timestr = time.strftime("%Y-%m-%d %H:%M:%S")
    log = f"[{timestr}] {msg}"
    print(log)
    with open("run_log.txt", "a", encoding="utf-8") as f:
        f.write(log + "\n")

def get_server_time():
    return datetime.now().strftime("%H:%M:%S")

def clean_timeout_clients():
    now = time.time()
    for addr in list(client_states.keys()):
        if now - client_states[addr]["last_active"] > TIMEOUT * 2:
            del client_states[addr]
            write_log(f"[{addr}] 客户端超时，清理连接")

def flush_partial_buffers():
    now = time.time()
    for addr, state in client_states.items():
        if not state.get("connected"): continue
        if state.get("buffer") and (now - state.get("last_active", 0) > FLUSH_TIMEOUT):
            srv_time = get_server_time()
            try:
                with open("received_from_client.txt", "ab") as rf:
                    for s, m in state["buffer"]:
                        rf.write(m)
            except Exception as e:
                write_log(f"写入失败（flush）: {e}")

            ack_val = state["expected_seq"] - 1
            ack_msg = f"ACK|{ack_val}|{srv_time}"
            ack_bytes = ack_msg.encode("utf-8")
            resp = struct.pack("!BHH", 1, state["sid"], len(ack_bytes)) + ack_bytes
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

    try:
        cmd, sid, dlen = struct.unpack("!BHH", raw_data[:5])
        payload = raw_data[5:5+dlen]
    except:
        continue

    real_stu = sid ^ KEY
    if not (0 <= real_stu <= 9999): continue

    client_states[addr]["last_active"] = time.time()
    state = client_states[addr]

    # 连接
    if cmd == 0:
        state["connected"] = True
        state["expected_seq"] = 0
        state["last_ack"] = -1
        state["sid"] = sid
        srv.sendto(struct.pack("!BHH", 0, sid, 0), addr)
        write_log(f"[{addr}] 学生 {real_stu} 连接成功")

    # 数据报文（二进制 80 字节）
    elif cmd == 1:
        if not state["connected"]: continue

        # ==================== 正确解析：seq|二进制数据 ====================
        try:
            payload_str = payload.decode("utf-8", errors="ignore")
            seq_str, content_bin = payload_str.split("|", 1)
            seq = int(seq_str)
            content = content_bin.encode("utf-8")
        except:
            continue

        srv_time = get_server_time()
        if random.random() < DROP_RATE:
            write_log(f"[{addr}] 丢包 seq={seq}")
            continue

        # 按序到达：立即ACK
        if seq == state["expected_seq"]:
            state["buffer"].append((seq, content))
            state["expected_seq"] += 1
            write_log(f"[{addr}] 接收 seq={seq} 缓冲={len(state['buffer'])}")

            # 立刻回ACK
            ack_msg = f"ACK|{seq}|{srv_time}"
            ack_bytes = ack_msg.encode("utf-8")
            resp = struct.pack("!BHH", 1, sid, len(ack_bytes)) + ack_bytes
            srv.sendto(resp, addr)
            state["last_ack"] = seq

            if len(state["buffer"]) >= GBN_WINDOW_SIZE:
                with open("received_from_client.txt", "ab") as f:
                    for s, m in state["buffer"]:
                        f.write(m)
                state["buffer"].clear()

        else:
            # 重复/乱序：发最后ACK
            last_ack = state.get("last_ack", -1)
            ack_msg = f"ACK|{last_ack}|{srv_time}"
            resp = struct.pack("!BHH", 1, sid, len(ack_msg.encode())) + ack_msg.encode()
            srv.sendto(resp, addr)

    # EOT 结束
    elif cmd == 2:
        if not state["connected"]: continue
        write_log(f"[{addr}] 收到 EOT")

        if state["buffer"]:
            with open("received_from_client.txt", "ab") as f:
                for s, m in state["buffer"]:
                    f.write(m)
            state["buffer"].clear()
        

        ack_val = state["expected_seq"] - 1
        ack_msg = f"ACK|{ack_val}|{get_server_time()}"
        resp = struct.pack("!BHH", 1, sid, len(ack_msg.encode())) + ack_msg.encode()
        srv.sendto(resp, addr)
        state["last_ack"] = ack_val

srv.close()
write_log("✅ 服务器已正常关闭")
