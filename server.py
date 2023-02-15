import sys
import socket

Message_size=312646
MAX_RECV_SIZE = 4*1024
test_time = 10

def train(ps_ip,ps_port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((ps_ip, ps_port))
    sock.listen(1)

    client_sock, (client_ip, _) = sock.accept()
    i = 0
    while(i < test_time):
        buffer = bytearray()
        while len(buffer) < Message_size:
            buffer += client_sock.recv(MAX_RECV_SIZE)
        client_sock.send(bytes(Message_size))
        i = i +1

if __name__ == '__main__':
    ps_ip = sys.argv[1]
    ps_port = int(sys.argv[2])
    train(ps_ip, ps_port)