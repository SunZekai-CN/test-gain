import sys
import socket
import threading
import multiprocessing as mp
import pickle
import time
from train_worker import Model_size,Chunk_size,test_time,MAX_RECV_SIZE,num_worker,SSP_threshold,locked

def parameter_server_all(isupdate):
    training_step=list([0 for _ in range(num_worker)])
    slowest=0
    while True:
        rank=isupdate[-1].get()
        training_step[rank]=training_step[rank]+1
        now_slowest=min(training_step)
        if slowest!=now_slowest:
            for i in range(num_worker):
                if i ==rank:
                    continue
                if training_step[i]==now_slowest+SSP_threshold:
                    isupdate[i].put("ok")
            slowest=now_slowest
        if training_step[rank]<=now_slowest+SSP_threshold:
            isupdate[rank].put("ok")
        print(f"{training_step}",flush=True)
    
def each_parameter_server(tcpsock,transport,ps_ip,ps_port,i,isupdate,lock):
    if transport=="tcp":
        while(True):
            buffer = bytearray()
            msg=pickle.loads(tcpsock.recv(MAX_RECV_SIZE))
            print(msg)
            if locked:
                lock.acquire()
            tcpsock.send(pickle.dumps("start"))
            print(f"{i} start transmit {time.time()}")
            start=time.time()
            while len(buffer) < Model_size:
                buffer += tcpsock.recv(MAX_RECV_SIZE)
            end=time.time()
            print(f"{i} transmit {end-start} s")
            if locked:
                lock.release()
            start=time.time()
            isupdate[-1].put(i)
            msg=isupdate[i].get()
            end=time.time()
            print(f"{i} waiting {end-start} s at {time.time()}",flush=True)
            assert msg=="ok"
            if locked:
                lock.acquire()
            print(f"{i} start send model at {time.time()}")
            tcpsock.send(bytes(Model_size))
            if locked:
                lock.release()
    else:
        udpsock=socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udpsock.bind((ps_ip,ps_port+i+1))
    if transport=="udp":
        while(True):
            buffer = bytearray()
            while True:
                try:
                    msg=pickle.loads(tcpsock.recv(MAX_RECV_SIZE))
                    print(msg)
                    if msg=="send":
                        break
                except:
                    continue
            if locked:
                lock.acquire()
            tcpsock.send(pickle.dumps("start"))
            while True: 
                data,addr = udpsock.recvfrom(MAX_RECV_SIZE)
                if len(data)<Chunk_size:
                    msg=pickle.loads(data)
                    print(msg)
                    if len(buffer)!=0:
                        tcpsock.send(pickle.dumps("ok"))
                        break
                else:
                    buffer+=data
            if locked:
                lock.release()
            isupdate[-1].put(i)
            msg=isupdate[i].get()
            if locked:
                lock.acquire()
            for _ in range(Model_size//Chunk_size+1):
                udpsock.sendto(bytes(Chunk_size),addr)
            udpsock.sendto(pickle.dumps("ok"),addr)
            while True:
                try:
                    msg=pickle.loads(tcpsock.recv(MAX_RECV_SIZE))
                    print(msg)
                    if msg=="ok":
                        break
                except:
                    udpsock.sendto(pickle.dumps("ok"),addr)
            if locked:
                lock.release()
                
    if transport=="reliable":
        while(True):
            buffer = bytearray()
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
            isupdate[-1].put(i)
            msg=isupdate[i].get()
            assert msg=="ok"
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
                        print(sent_data)
                        break
                    except:
                        continue
                print(f"get sent {sent_data}")
                if sent_data>=Model_size:
                    break
def train(ps_ip,ps_port,transport):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((ps_ip, ps_port))
    sock.listen()
    lock=threading.Lock()
    manager=mp.Manager()
    proc=[]
    isupdate=[]
    for i in range(num_worker):
        isupdate.append(mp.Queue(maxsize=100))
    isupdate.append(mp.Queue(maxsize=100))
    for i in range(num_worker):
        client_sock, (client_ip, _) = sock.accept()
        t=threading.Thread(target=each_parameter_server,args=(client_sock,transport,ps_ip,ps_port,i,isupdate,lock))
        proc.append(t)
    t=threading.Thread(target=parameter_server_all,args=(isupdate,))
    proc.append(t)
    for t in proc:
        t.start()
    for t in proc:
        t.join()

if __name__ == '__main__':
    ps_ip = sys.argv[1]
    ps_port = int(sys.argv[2])
    transport=sys.argv[3]
    train(ps_ip, ps_port,transport)