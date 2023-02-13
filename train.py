import sys
import socket
import threading
from train_worker import Message_size,MAX_RECV_SIZE

def train(ps_ip,ps_port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((ps_ip, ps_port))
    sock.listen(1)
    lock=threading.Lock()
    while(True):
        buffer = bytearray()
        while len(buffer) < Message_size:
            buffer += sock.recv(MAX_RECV_SIZE)
        sock.send(bytes(Message_size))

if __name__ == '__main__':
    ps_ip = sys.argv[1]
    ps_port = int(sys.argv[2])
    train(ps_ip, ps_port)