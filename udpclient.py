import socket
import struct
import time
import pandas as pd
from collections import defaultdict
import random

# ==================== 配置 ====================
SERVER_IP = "172.18.203.113"
PORT = 9999
KEY = 0x5A3C
STUDENT_ID = 2119
TIMEOUT = 0.3

MIN_PKT_SIZE = 40
MAX_PKT_SIZE = 80
MAX_WINDOW_BYTES = 400  # 窗口最大字节数
PACKET_FIXED_SIZE = 80   # 用于打印字节范围
# ==============================================

SID = STUDENT_ID ^ KEY
client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
client.settimeout(TIMEOUT)

# 统计全局变量
send_count = {}
total_send_pkts = 0
rtt_list = []
dup_ack_count = defaultdict(int)
timeout_retrans_count = 0
fast_retrans = 0

def write_log(msg):
    timestr = time.strftime("%Y-%m-%d %H:%M:%S.") + f"{int(time.time() * 1000) % 1000:03d}"
    log = f"[{timestr}] {msg}"
    print(log)
    with open("client_log.txt", "a", encoding="utf-8") as f:
        f.write(log + "\n")

def connect():
    pkt = struct.pack("!BHHh", 0, SID, 0, 0)
    client.sendto(pkt, (SERVER_IP, PORT))
    try:
        resp, _ = client.recvfrom(1024)
        cmd, _, _, _ = struct.unpack("!BHHh", resp[:7])
        return cmd == 0
    except:
        return False

def load_and_split_file():
    try:
        with open("source.txt", "rb") as f:
            data = f.read()
    except:
        print("❌ 请先创建 source.txt 文件！")
        exit()

    packets = []
    seq = 0
    idx = 0
    total_len = len(data)
    while idx < total_len:
        remain = total_len - idx
        chunk_size = min(random.randint(MIN_PKT_SIZE, MAX_PKT_SIZE), remain)
        chunk = data[idx:idx+chunk_size]
        packets.append((seq, chunk))
        seq += 1
        idx += chunk_size

    print(f"✅ 文件读取完成，共切分成 {len(packets)} 个包")
    print(f"✅ 每包长度在 {MIN_PKT_SIZE}~{MAX_PKT_SIZE} 字节间随机（最后一包可短）\n")
    return packets

# ==================== 新增：计算窗口结束序号 ====================
def calc_window_end(start_seq, packets):
    total_bytes = 0
    end_seq = start_seq
    while end_seq < len(packets):
        pkt_size = len(packets[end_seq][1])
        if total_bytes + pkt_size > MAX_WINDOW_BYTES:
            break
        total_bytes += pkt_size
        end_seq += 1
    return end_seq

def gbn_send_file_packets(packets):
    global total_send_pkts, timeout_retrans_count, fast_retrans

    base = 0
    next_seq = 0
    max_seq = len(packets)
    send_time = {}
    acked = [False] * max_seq

    while base < max_seq:
        # 计算当前窗口结束序号
        window_end = calc_window_end(base, packets)
        window_bytes = sum(len(packets[i][1]) for i in range(base, window_end))
        print(f"\n窗口范围={base}~{window_end-1} 包数={window_end-base} 总长度={window_bytes}B")

        # 发送窗口内的数据包
        while next_seq < window_end:
            seq, content = packets[next_seq]
            pkt = struct.pack("!BHHh", 1, SID, len(content), seq) + content
            client.sendto(pkt, (SERVER_IP, PORT))

            send_count[seq] = send_count.get(seq, 0) + 1
            total_send_pkts += 1
            send_time[seq] = time.time()

            start = seq * PACKET_FIXED_SIZE
            end = start + len(content) - 1
            print(f"第 {seq} 个（{start}~{end} 字节）发送")
            write_log(f"发包 seq={seq}")

            next_seq += 1

        # 等待 ACK
        try:
            resp, _ = client.recvfrom(2048)
            cmd, sid, dlen, ack = struct.unpack("!BHHh", resp[:7])
            now = time.time()

            # 更新 ACK
            for s in range(base, ack + 1):
                if s >= 0 and s < max_seq and not acked[s]:
                    acked[s] = True
                    if s in send_time:
                        rtt = int((now - send_time[s]) * 1000)
                        rtt_list.append(rtt)
                        recent = rtt_list[-5:]
                        avg_rtt = sum(recent)/len(recent)
                        client.settimeout(max(avg_rtt*2/1000, 0.1))
                        start = s * PACKET_FIXED_SIZE
                        end = start + len(packets[s][1]) - 1
                        print(f"第 {s} 个 server 收到，RTT={rtt}ms")
                        write_log(f"收包 seq={s}, RTT={rtt}ms")

            # 快速重传
            if (base == 0 and ack == -1) or (ack == base-1):
                dup_ack_count[ack] += 1
                if dup_ack_count[ack] >= 2:
                    print(f"🚀 快速重传 window={base}~{window_end-1}")
                    write_log(f"快速重传 window={base}~{window_end-1}")
                    fast_retrans += 1
                    dup_ack_count[ack] = 0
                    for seq in range(base, window_end):
                        s, content = packets[seq]
                        pkt = struct.pack("!BHHh",1,SID,len(content),s)+content
                        client.sendto(pkt,(SERVER_IP,PORT))
                        send_count[s] += 1
                        total_send_pkts += 1
                        send_time[s] = time.time()
                        write_log(f"重传 seq={s}")

            # 滑动窗口
            while base < max_seq and acked[base]:
                base += 1
            next_seq = max(next_seq, base)

        except socket.timeout:
            print(f"⏰ 超时！重传 window={base}~{window_end-1}")
            write_log(f"超时 window={base}~{window_end-1}")
            timeout_retrans_count += 1
            for seq in range(base, window_end):
                s, content = packets[seq]
                pkt = struct.pack("!BHHh",1,SID,len(content),s)+content
                client.sendto(pkt,(SERVER_IP,PORT))
                send_count[s] += 1
                total_send_pkts += 1
                send_time[s] = time.time()
                write_log(f"重传 seq={s}")

    # 发送 EOT
    eot_pkt = struct.pack("!BHHh",2,SID,3,0)+b"EOT"
    client.sendto(eot_pkt,(SERVER_IP,PORT))

    print("\n📊 传输汇总统计")
    EXPECTED_PKT = len(packets)
    loss_rate = (1 - EXPECTED_PKT/total_send_pkts)*100 if total_send_pkts>0 else 0
    df = pd.Series(rtt_list)
    print(f"总包数: {EXPECTED_PKT}, 总发送: {total_send_pkts}, 丢包率: {loss_rate:.2f}%")
    print(f"最大RTT: {df.max()} ms, 最小RTT: {df.min()} ms, 平均RTT: {df.mean():.2f} ms, RTT std: {df.std():.2f} ms")

if __name__ == "__main__":
    if connect():
        print("✅ 连接服务端成功")
        pkts = load_and_split_file()
        gbn_send_file_packets(pkts)
    else:
        print("❌ 连接服务端失败")