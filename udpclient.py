import socket
import struct
import time
import pandas as pd

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
send_count = {}  # 记录每个seq发送次数（用于丢包率）
total_send_pkts = 0
rtt_list = []

def connect():
    pkt = struct.pack("!BHH", 0, SID, 0)
    client.sendto(pkt, (SERVER_IP, PORT))
    try:
        resp, _ = client.recvfrom(1024)
        cmd, _, _ = struct.unpack("!BHH", resp[:5])
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
    global total_send_pkts
    base = 0
    next_seq = 0
    max_seq = len(packets)
    send_time = {}
    acked = [False] * max_seq

    print("=== 开始 按批次（最多5个）发送，每包固定80字节 ===\n")

    while base < max_seq:
        # 一次性发满窗口 5 个
        while next_seq < max_seq and next_seq < base + WINDOW_PKT_BATCH:
            seq, content = packets[next_seq]
            payload = f"{seq}|".encode() + content
            pkt = struct.pack("!BHH", 1, SID, len(payload)) + payload
            client.sendto(pkt, (SERVER_IP, PORT))

            # 统计发送次数
            if seq not in send_count:
                send_count[seq] = 0
            send_count[seq] += 1
            total_send_pkts += 1

            send_time[seq] = time.time()
            print(f"发送 seq={seq}")
            next_seq += 1

        # 接收 ACK
        try:
            resp, _ = client.recvfrom(2048)
            cmd, _, dlen = struct.unpack("!BHH", resp[:5])
            payload = resp[5:5+dlen].decode()

            if cmd == 1 and payload.startswith("ACK"):
                parts = payload.split("|")
                ack = int(parts[1])
                now = time.time()

                if ack in send_time and not acked[ack]:
                    rtt = int((now - send_time[ack]) * 1000)
                    rtt_list.append(rtt)
                    acked[ack] = True
                    start = ack * 80
                    end = start + len(packets[ack][1])
                    print(f"\n第 {ack+1} 个（第 {start}~{end} 字节）server 端已经收到，RTT 是 {rtt} ms")

                # 滑动窗口
                while base < max_seq and acked[base]:
                    base += 1

        except socket.timeout:
            print(f"\n⏰ 超时！重传第 {base}~{next_seq-1} 号数据包")
            for seq in range(base, next_seq):
                s, content = packets[seq]
                payload = f"{s}|".encode() + content
                pkt = struct.pack("!BHH", 1, SID, len(payload)) + payload
                client.sendto(pkt, (SERVER_IP, PORT))

                send_count[s] += 1
                total_send_pkts += 1
                send_time[s] = time.time()
                print(f"重传 seq={s}")

    # 发送 EOT
    print("\n发送 EOT（表示无更多数据）")
    eot_pkt = struct.pack("!BHH", 2, SID, 3) + b"EOT"
    client.sendto(eot_pkt, (SERVER_IP, PORT))

    # ===================== (8) 汇总统计 =====================
    print("\n" + "="*50)
    print("📊 传输汇总统计")
    print("="*50)

    EXPECTED_PKT = len(packets)
    丢包率 = (1 - EXPECTED_PKT / total_send_pkts) * 100

    df = pd.Series(rtt_list)
    max_rtt = df.max()
    min_rtt = df.min()
    avg_rtt = df.mean()
    std_rtt = df.std()

    print(f"总包数（期望）: {EXPECTED_PKT}")
    print(f"实际发送包数: {total_send_pkts}")
    print(f"丢包率: {丢包率:.2f}%")
    print(f"最大RTT: {max_rtt} ms")
    print(f"最小RTT: {min_rtt} ms")
    print(f"平均RTT: {avg_rtt:.2f} ms")
    print(f"RTT标准差: {std_rtt:.2f} ms")
    print("="*50)

if __name__ == "__main__":
    if connect():
        print("✅ 连接服务端成功")
        pkts = load_and_split_file()
        gbn_send_file_packets(pkts)
    else:
        print("❌ 连接服务端失败")