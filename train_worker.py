import sys
import socket
import time
import numpy as np
import pickle
#import zmq

Model_size=6731196*4
Chunk_size=1400
test_time=100
MAX_RECV_SIZE = 4*1024
num_worker=3
SSP_threshold=0
locked=False
def train_worker(ps_ip, ps_port,transport,offset):
    tcpsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcpsock.connect((ps_ip, ps_port))
    udpsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    i = 0
    network_waiting = []
    buffer = bytearray()
    print("connection complete")
    if transport=="tcp":
        while(i<test_time):
            time.sleep(1.2)
            start=time.time()
            tcpsock.send(pickle.dumps("send"))
            msg=pickle.loads(tcpsock.recv(MAX_RECV_SIZE))
            print(msg)
            print(f"start send at {time.time()}")
            tcpsock.send(bytes(Model_size))
            while len(buffer) < Model_size:
                buffer += tcpsock.recv(MAX_RECV_SIZE)
            end=time.time()
            print(f"at {end} get model")
            if i>5:
                network_waiting.append(end-start)
                print(f'iteration {i} {network_waiting[-1]},get {len(buffer)} ', flush=True)
                print(f"average {np.mean(network_waiting)}",flush=True)
            buffer = bytearray()
            i = i+1
    if transport=="udp":
        tcpsock.settimeout(0.005)
        while(i<test_time):
            time.sleep(1.2)
            start=time.time()
            tcpsock.send(pickle.dumps("send"))
            while True:
                try:
                    msg=pickle.loads(tcpsock.recv(MAX_RECV_SIZE))
                    print(msg)
                    if msg=="start":
                        break
                except:
                    continue
            for _ in range(Model_size//Chunk_size+1):
                udpsock.sendto(bytes(Chunk_size),(ps_ip,ps_port+offset))
            udpsock.sendto(pickle.dumps("ok"),(ps_ip,ps_port+offset))
            while True:
                try:
                    msg=pickle.loads(tcpsock.recv(MAX_RECV_SIZE))
                    print(msg)
                    if msg=="ok":
                        break
                except:
                    udpsock.sendto(pickle.dumps("ok"),(ps_ip,ps_port+offset))    
            buffer = bytearray()
            while True: 
                data,addr = udpsock.recvfrom(MAX_RECV_SIZE)
                if len(data)<Chunk_size:
                    msg=pickle.loads(data)
                    tcpsock.send(pickle.dumps("ok"))
                    print(msg)
                    if len(buffer)!=0:
                        break
                else:
                    buffer+=data
            end=time.time()
            if i>0:
                network_waiting.append(end-start)
                print(f'iteration {i} {network_waiting[-1]},get {len(buffer)} {(1-len(buffer)/Model_size)*100}', flush=True)
                print(f"average {np.mean(network_waiting)}",flush=True)
            i = i+1
        
    if transport=="reliable":
        tcpsock.setblocking(False)
        while(i<test_time):
            start=time.time()
            sent_data=0
            while True:
                tcpsock.send(pickle.dumps("start"))
                for _ in range((Model_size-sent_data)//Chunk_size+1):
                    udpsock.sendto(bytes(Chunk_size),(ps_ip,ps_port+1))
                tcpsock.send(pickle.dumps("end"))
                while True:
                    try:
                        data=tcpsock.recv(MAX_RECV_SIZE)
                        sent_data=pickle.loads(data)
                        break
                    except:
                        continue
                print(f"get sent {sent_data}")
                if sent_data>=Model_size:
                    break
            tcpsock.setblocking(True)
            while len(buffer) < Model_size:
                msg=pickle.loads(tcpsock.recv(MAX_RECV_SIZE))
                print(msg)
                tcpsock.setblocking(False)
                while True: 
                    try:   
                        data=tcpsock.recv(MAX_RECV_SIZE)
                        msg=pickle.loads(data)
                        print(msg)
                        if msg=="end":
                            break
                    except:
                        data,addr = udpsock.recvfrom(MAX_RECV_SIZE)
                        buffer+=data
                tcpsock.setblocking(True)
                tcpsock.send(pickle.dumps(len(buffer)))
            end=time.time()
            network_waiting.append(end-start)
            print(f'iteration {i} {network_waiting[-1]},get {len(buffer)}', flush=True)
            buffer = bytearray()
            i = i+1
        print(f"average {np.mean(network_waiting)}",flush=True)

if __name__ == '__main__':
    ps_ip = sys.argv[1]
    ps_port = int(sys.argv[2])
    transport=sys.argv[3]
    rank=sys.argv[4]
    train_worker(ps_ip, ps_port,transport,int(rank))
