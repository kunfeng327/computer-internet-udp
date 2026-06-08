import socket
import struct
import time
import pandas as pd
from collections import defaultdict

# ==================== 配置 ====================
SERVER_IP = "172.18.203.113"
PORT = 9999
KEY = 0x5A3C
STUDENT_ID = 2119
TIMEOUT = 0.3

PACKET_FIXED_SIZE = 80
WINDOW_PKT_BATCH = 5
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

def connect():
    # ====================== 7B 首部 !BHHH ======================
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
        end = min(idx + PACKET_FIXED_SIZE, total_len)
        chunk = data[idx:end]
        packets.append((seq, chunk))
        seq += 1
        idx = end

    print(f"✅ 文件读取完成，共切分成 {len(packets)} 个包")
    print(f"✅ 每包固定为 {PACKET_FIXED_SIZE} 字节（最后一包可短）\n")
    return packets

def gbn_send_file_packets(packets):
    global total_send_pkts, timeout_retrans_count
    fast_retrans = 0
    base = 0
    next_seq = 0
    max_seq = len(packets)
    send_time = {}
    acked = [False] * max_seq

    print("=== 开始 按批次（最多5个）发送，每包固定80字节 ===\n")

    while base < max_seq:
        while next_seq < max_seq and next_seq < base + WINDOW_PKT_BATCH:
            seq, content = packets[next_seq]
            # ====================== 7B 首部 !BHHh ======================
            payload = content
            pkt = struct.pack("!BHHh", 1, SID, len(payload), seq) + payload
            client.sendto(pkt, (SERVER_IP, PORT))

            if seq not in send_count:
                send_count[seq] = 0
            send_count[seq] += 1
            total_send_pkts += 1

            send_time[seq] = time.time()
            start = seq * PACKET_FIXED_SIZE
            end = start + len(packets[seq][1]) - 1
            print(f"第 {seq} 个（第 {start}~{end} 字节）client 端已经发送")
            next_seq += 1

        try:
            resp, _ = client.recvfrom(2048)
            # ====================== 7B 解包 ======================
            cmd, sid, dlen, ack = struct.unpack("!BHHh", resp[:7])
            payload = resp[7:7+dlen]
            now = time.time()

            for s in range(base, ack + 1):
                if s >=0 and s < max_seq and not acked[s]:
                    acked[s] = True
                    if s in send_time:
                        rtt = int((now - send_time[s]) * 1000)
                        rtt_list.append(rtt)
                        start = s * PACKET_FIXED_SIZE
                        end = start + len(packets[s][1]) - 1
                        print(f"\n第 {s} 个（第 {start}~{end} 字节）server 端已经收到，RTT 是 {rtt} ms")

            if (base == 0 and ack == -1) or (ack == base - 1):
                dup_ack_count[ack] +=1
                print(f"🔁 收到重复ACK: {ack} (累计 {dup_ack_count[ack]} 次)")
                if dup_ack_count[ack] >= 3:
                    print(f"\n🚀 快速重传 整个窗口 seq={base} ~ {next_seq-1}")
                    fast_retrans += 1
                    dup_ack_count[ack] = 0
                    for seq in range(base, next_seq):
                        s, content = packets[seq]
                        # ====================== 7B 首部 ======================
                        payload = content
                        pkt = struct.pack("!BHHh", 1, SID, len(payload), s) + payload
                        client.sendto(pkt, (SERVER_IP, PORT))
                        send_count[s] += 1
                        total_send_pkts += 1
                        send_time[s] = time.time()
                        start = s * PACKET_FIXED_SIZE
                        end = start + len(packets[s][1]) - 1
                        print(f"重传第 {s} 个（第 {start}~{end} 字节）数据包。")

            while base < max_seq and acked[base]:
                base += 1

            while next_seq < max_seq and next_seq < base + WINDOW_PKT_BATCH:
                seq, content = packets[next_seq]
                # ====================== 7B 首部 ======================
                payload = content
                pkt = struct.pack("!BHHh", 1, SID, len(payload), seq) + payload
                client.sendto(pkt, (SERVER_IP, PORT))

                if seq not in send_count:
                    send_count[seq] = 0
                send_count[seq] += 1
                total_send_pkts += 1
                send_time[seq] = time.time()
                start = seq * PACKET_FIXED_SIZE
                end = start + len(packets[seq][1]) - 1
                print(f"第 {seq} 个（第 {start}~{end} 字节）client 端已经发送")
                next_seq += 1

        except socket.timeout:
            print(f"\n⏰ 超时！重传第 {base}~{next_seq-1} 号数据包")
            timeout_retrans_count += 1
            for seq in range(base, next_seq):
                s, content = packets[seq]
                # ====================== 7B 首部 ======================
                payload = content
                pkt = struct.pack("!BHHh", 1, SID, len(payload), s) + payload
                client.sendto(pkt, (SERVER_IP, PORT))

                send_count[s] += 1
                total_send_pkts += 1
                send_time[s] = time.time()
                start = s * PACKET_FIXED_SIZE
                end = start + len(packets[s][1]) - 1
                print(f"重传第 {s} 个（第 {start}~{end} 字节）数据包。")

    print("\n发送 EOT（表示无更多数据）")
    # ====================== 7B EOT ======================
    eot_pkt = struct.pack("!BHHh", 2, SID, 3, 0) + b"EOT"
    client.sendto(eot_pkt, (SERVER_IP, PORT))

    print("\n📊 传输汇总统计")
    EXPECTED_PKT = len(packets)
    total_lost = fast_retrans + timeout_retrans_count
    丢包率 = (total_lost / (total_lost + EXPECTED_PKT)) * 100 if (total_lost + EXPECTED_PKT) > 0 else 0
    df = pd.Series(rtt_list)
    max_rtt = df.max()
    min_rtt = df.min()
    avg_rtt = df.mean()
    std_rtt = df.std()

    print(f"总包数（期望）: {EXPECTED_PKT}")
    print(f"丢包数: {total_lost}, 包括 {fast_retrans} 个快速重传和 {timeout_retrans_count} 个超时重传")
    print(f"丢包率: {丢包率:.2f}%")
    print(f"最大RTT: {max_rtt} ms")
    print(f"最小RTT: {min_rtt} ms")
    print(f"平均RTT: {avg_rtt:.2f} ms")
    print(f"RTT标准差: {std_rtt:.2f} ms")

if __name__ == "__main__":
    if connect():
        print("✅ 连接服务端成功")
        pkts = load_and_split_file()
        gbn_send_file_packets(pkts)
    else:
        print("❌ 连接服务端失败")