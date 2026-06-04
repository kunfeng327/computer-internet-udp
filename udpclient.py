import socket
import struct
import time
import random

# ==================== 【你可以自由改的配置】 ====================
SERVER_IP = "172.18.203.113"
PORT = 9999
KEY = 0x5A3C
STUDENT_ID = 2119
WINDOW_SIZE = 3      # 滑动窗口大小
TIMEOUT = 0.3        # 超时300ms
MIN_PKT_SIZE = 10    # 最小包长
MAX_PKT_SIZE = 20    # 最大包长
# ==============================================================

SID = STUDENT_ID ^ KEY
client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
client.settimeout(TIMEOUT)

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
        with open("source.txt", "r", encoding="utf-8") as f:
            data = f.read()
    except:
        print("❌ 请先创建 source.txt 文件！")
        exit()

    packets = []
    seq = 0
    idx = 0
    total_len = len(data)

    while idx < total_len:
        slice_size = random.randint(MIN_PKT_SIZE, MAX_PKT_SIZE)
        end = min(idx + slice_size, total_len)
        chunk = data[idx:end]
        packets.append((seq, chunk))
        seq += 1
        idx = end

    print(f"✅ 文件读取完成，共切分成 {len(packets)} 个包")
    print(f"✅ 包大小范围：{MIN_PKT_SIZE}~{MAX_PKT_SIZE} 字节\n")
    return packets

def gbn_send_file_packets(packets):
    base = 0
    next_seq = 0
    max_seq = len(packets)
    send_time = {}
    # 用于指示是否已发送过 EOT（客户端表示无更多数据）——不必持久化，只在发送循环中检查
    # 但逻辑为：每次当 next_seq 达到 max_seq（所有数据已发出）时，发送 EOT

    print("=== 开始 GBN 批量发送 ===")

    while base < max_seq:
        # ====================== 【修复】必须填满整个窗口 ======================
        while next_seq < base + WINDOW_SIZE and next_seq < max_seq:
            seq, content = packets[next_seq]
            payload = f"{seq}|{content}".encode("utf-8")
            pkt = struct.pack("!BHH", 1, SID, len(payload)) + payload

            client.sendto(pkt, (SERVER_IP, PORT))
            send_time[seq] = time.time()
            print(f"发送 seq={seq} | 包长度={len(content)}")
            next_seq += 1

        # 如果所有数据已发送（next_seq >= max_seq），告诉服务端没有更多数据（EOT）
        if next_seq >= max_seq:
            eot_payload = b"EOT"
            eot_pkt = struct.pack("!BHH", 2, SID, len(eot_payload)) + eot_payload
            try:
                client.sendto(eot_pkt, (SERVER_IP, PORT))
                print("发送 EOT（表示无更多数据）")
            except Exception as e:
                print(f"发送 EOT 失败: {e}")

        # ====================== 等待 ACK ======================
        try:
            resp, _ = client.recvfrom(2048)
            cmd, _, dlen = struct.unpack("!BHH", resp[:5])
            payload = resp[5:5+dlen].decode()

            if cmd == 1 and payload.startswith("ACK"):
                _, ack, _ = payload.split("|")
                ack = int(ack)
                rtt = int((time.time() - send_time[base]) * 1000)

                old_left = base
                old_right = base + WINDOW_SIZE - 1
                base = ack + 1
                new_left = base
                new_right = base + WINDOW_SIZE - 1

                print(f"\n✅ 收到 ACK={ack} | 窗口 [{old_left}~{old_right}] → [{new_left}~{new_right}] | RTT={rtt}ms")

        # ====================== 超时重传 ======================
        except socket.timeout:
            print(f"\n⏰ 超时！重传窗口 [{base} ~ {base + WINDOW_SIZE - 1}]")
            next_seq = base  # 【修复】回到窗口起点重传

    print("\n🎉 文件所有包发送完成！")

if __name__ == "__main__":
    if connect():
        print("✅ 连接服务端成功")
        pkts = load_and_split_file()
        gbn_send_file_packets(pkts)
    else:
        print("❌ 连接服务端失败")