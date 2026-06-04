import socket
import struct

VM_IP = "172.18.203.113"
PORT = 9999
KEY = 0x5A3C
stu_last4 = 1234
stu_sid = stu_last4 ^ KEY

cli = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# 先发连接请求
conn_head = struct.pack("!BHH", 0, stu_sid, 0)
cli.sendto(conn_head, (VM_IP, PORT))
resp, _ = cli.recvfrom(1024)
rcmd,_,_ = struct.unpack("!BHH", resp[:5])
if rcmd == 0xff:
    print("连不上，学号校验失败咯")
    exit()
print("连上服务器啦，随便打字发消息，输入quit退出")

# 循环发消息
while True:
    text = input("你要发的内容：")
    if text == "quit":
        break
    send_buf = text.encode("utf-8")
    pkt = struct.pack("!BHH",1,stu_sid,len(send_buf)) + send_buf
    cli.sendto(pkt,(VM_IP,PORT))
    res,_ = cli.recvfrom(1024)
    cmd,sid,l = struct.unpack("!BHH",res[:5])
    print("服务器回：", res[5:5+l].decode("utf-8"))

cli.close()