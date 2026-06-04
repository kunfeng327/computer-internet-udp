import socket
import struct

# 配置
BIND_IP = "0.0.0.0"
PORT = 9999
KEY = 0x5A3C  # 固定异或值

# 标记：客户端是否已经完成连接
client_conn = dict()  # {客户端地址: 是否连接成功}

srv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

srv.bind((BIND_IP, PORT))
print("UDP服务端启动，监听9999，等待客户端建链...")

while True:
    raw_data, addr = srv.recvfrom(2048)
    try:
        # 解包头：1B cmd + 2B sid + 2B datalen
        cmd, sid, dlen = struct.unpack("!BHH", raw_data[:5])
        payload = raw_data[5:5+dlen]
    except Exception as e:
        print("报文格式错误", addr)
        continue

    # --------StudentID校验核心--------
    real_last4 = sid ^ KEY
    if not (0 <= real_last4 <= 9999):
        print(f"【{addr}】StudentID校验失败，拒绝连接！sid={sid},还原={real_last4}")
        # 返回拒绝报文 cmd=0xff
        resp = struct.pack("!BHH", 0xff, 0, 0)
        srv.sendto(resp, addr)
        continue

    # cmd=0：客户端发起连接请求
    if cmd == 0:
        print(f"【{addr}】连接请求，学号后四位：{real_last4}，校验通过")
        client_conn[addr] = True
        # 服务端回复同意连接
        resp_head = struct.pack("!BHH", 0, sid, 0)
        srv.sendto(resp_head, addr)

    # cmd=1：业务数据，必须先建立连接
    elif cmd == 1:
        if client_conn.get(addr, False):
            msg = payload.decode("utf-8")
            print(f"【{addr}】收到业务数据：{msg}")
            back = "服务端已收到数据".encode("utf-8")
            resp = struct.pack("!BHH",1,sid,len(back)) + back
            srv.sendto(resp,addr)
        else:
            print(f"【{addr}】未建立连接，禁止发数据")