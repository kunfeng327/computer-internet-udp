import socket
import struct
import random
import time

# 配置
BIND_IP = "0.0.0.0"
PORT = 9999
KEY = 0x5A3C
DROP_RATE = 0.1  # 丢包概率10%：0.1=10%几率丢包不回复

client_conn = dict()

# 日志写入函数，统一格式，时间戳用于匹配wireshark
def write_log(content):
    t = time.time()  # 秒级时间戳，和wireshark时间基准一致
    local_t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))
    log_str = f"[{local_t}] | 时间戳{round(t,6)} | {content}\n"
    with open("run_log.txt","a",encoding="utf-8") as f:
        f.write(log_str)
    print(log_str.strip())

srv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
srv.bind((BIND_IP, PORT))
write_log("UDP服务端启动，监听9999")

while True:
    raw_data, addr = srv.recvfrom(2048)
    try:
        cmd, sid, dlen = struct.unpack("!BHH", raw_data[:5])
        payload = raw_data[5:5+dlen]
    except Exception as e:
        write_log(f"{addr} 报文格式错误")
        continue

    real_last4 = sid ^ KEY
    if not (0 <= real_last4 <= 9999):
        info = f"{addr} StudentID校验失败，拒绝连接，还原学号尾{real_last4}"
        write_log(info)
        resp = struct.pack("!BHH", 0xff, 0, 0)
        srv.sendto(resp, addr)
        continue

    if cmd == 0:
        info = f"{addr} 连接请求，学号尾{real_last4}，校验通过"
        write_log(info)
        client_conn[addr] = True
        resp_head = struct.pack("!BHH", 0, sid, 0)
        srv.sendto(resp_head, addr)

    elif cmd == 1:
        if client_conn.get(addr, False):
            msg = payload.decode("utf-8")
            # ==========随机丢包核心代码==========
            rand_num = random.random() # 0~1随机小数
            if rand_num < DROP_RATE:
                # 随机丢包：收到了但是不回包
                write_log(f"{addr} 收到数据[{msg}]，随机丢包，放弃应答，随机值{rand_num:.3f}")
                continue
            # 没丢包：正常回复
            write_log(f"{addr} 收到数据[{msg}]，正常返回应答，随机值{rand_num:.3f}")
            back = "服务端已收到数据".encode("utf-8")
            resp = struct.pack("!BHH",1,sid,len(back)) + back
            srv.sendto(resp,addr)
        else:
            write_log(f"{addr} 未建立连接，禁止收发数据")