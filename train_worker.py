import sys
import socket
import time
import numpy as np
import pickle
#import zmq

Message_size=3195008
MAX_RECV_SIZE = 4*1024
test_time = 10

def train_worker(ps_ip, ps_port):
    tcpsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcpsock.connect((ps_ip, ps_port))
    
    i = 0
    network_waiting = []
    print("connection complete")
    while(i<test_time):
        time.sleep(1.2)
        buffer = bytearray()
        print("start send")
        start=time.time()
        tcpsock.send(bytes(Message_size))
        while len(buffer) < Message_size:
            buffer += tcpsock.recv(MAX_RECV_SIZE)
        end=time.time()
        network_waiting.append(end-start)
        print(f'iteration {i} {network_waiting[-1]},get {len(buffer)} bytes,{len(buffer)/1024/1024}MB ', flush=True)
        print(f"average {np.mean(network_waiting)}",flush=True)
        i = i+1
    
if __name__ == '__main__':
    ps_ip = sys.argv[1]
    ps_port = int(sys.argv[2])
    train_worker(ps_ip, ps_port)
