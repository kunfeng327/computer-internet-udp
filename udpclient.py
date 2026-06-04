import socket
import struct
import time

VM_IP = "172.18.203.113"
PORT = 9999
KEY = 0x5A3C
stu_last4 = 2119  # 改成你的学号后四位
stu_sid = stu_last4 ^ KEY
TIMEOUT = 1.0  # 1秒超时没回复就重传

# 日志函数
def write_log(content):
    t = time.time()
    local_t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))
    log_str = f"[{local_t}] | 时间戳{round(t,6)} | {content}\n"
    with open("run_log.txt", "a", encoding="utf-8") as f:
        f.write(log_str)
    print(log_str.strip())

# 创建UDP客户端
cli = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
cli.settimeout(TIMEOUT)

# ===================== 修复点1：连接必须成功才能继续 =====================
connected = False
while not connected:
    conn_head = struct.pack("!BHH", 0, stu_sid, 0)
    cli.sendto(conn_head, (VM_IP, PORT))
    write_log("客户端发送连接建立请求")
    
    try:
        resp, _ = cli.recvfrom(1024)
        rcmd, _, _ = struct.unpack("!BHH", resp[:5])
        
        if rcmd == 0xFF:
            write_log("连接被服务器拒绝，程序退出")
            cli.close()
            exit()
        
        write_log("连接建立成功，可以发送业务数据")
        connected = True
        
    except socket.timeout:
        write_log("连接超时，正在重试...")
    except Exception as e:
        write_log(f"连接异常：{e}")
        cli.close()
        exit()

# ===================== 主循环 =====================
while True:
    send_text = input("\n输入要发送的消息(quit退出)：").strip()
    if send_text == "quit":
        break
    if not send_text:
        print("消息不能为空")
        continue

    send_bytes = send_text.encode("utf-8")
    send_pkt = struct.pack("!BHH", 1, stu_sid, len(send_bytes)) + send_bytes

    re_cnt = 0
    success = False
    
    # 超时重传（最多重试5次，避免死循环）
    while re_cnt < 5:
        cli.sendto(send_pkt, (VM_IP, PORT))
        re_cnt += 1
        write_log(f"第{re_cnt}次发送：{send_text}")

        try:
            ans, _ = cli.recvfrom(1024)
            a_cmd, a_sid, a_len = struct.unpack("!BHH", ans[:5])

            # ===================== 修复点2：校验会话ID =====================
            if a_sid != stu_sid:
                write_log("错误：应答的会话ID不匹配，丢弃包")
                continue

            # ===================== 修复点3：防止长度为0 =====================
            if a_len <= 0 or len(ans) < 5 + a_len:
                write_log("错误：应答包格式无效")
                continue

            ans_msg = ans[5:5+a_len].decode("utf-8")
            write_log(f"收到服务端应答：{ans_msg}")
            success = True
            break

        except socket.timeout:
            write_log(f"第{re_cnt}次超时，准备重发")

    if not success:
        write_log("连续5次超时，服务器无响应")

# 退出
cli.close()
write_log("客户端已正常关闭")