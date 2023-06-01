import threading
from socket import *
from threading import Thread
import signal
import math
import multiprocessing


MANAGER_HOST = "localhost"
MANAGER_PORT = 23342
BUFF_SIZE = 4096

MANAGER_SOCK = socket(AF_INET, SOCK_STREAM)
MANAGER_SOCK.connect((MANAGER_HOST, MANAGER_PORT))

PEER_SOCK = socket(AF_INET, SOCK_STREAM)
PEER_SOCK.bind(("localhost", 0))

PEER_SOCK_ADDR = PEER_SOCK.getsockname()
PEER_SOCK_MSG = str(PEER_SOCK_ADDR[0])
PEER_SOCK_MSG += " "
PEER_SOCK_MSG += str(PEER_SOCK_ADDR[1])

# print(PEER_SOCK_MSG)

MANAGER_SOCK.send(PEER_SOCK_MSG.encode())


Fragments = []
Fragments_LOCK = threading.Lock()

TO_REDOWNLOAD = dict()
REDOWNLOADING = False

PEER_LIST = dict()
FILES = []
FILE_SIZE = dict()

FILE_PEER = dict()


def update_peers(msg):
    if msg != '':
        msg = msg.strip().split()

        new_keys = []
        # print(msg)
        for i in range(0, len(msg), 2):
            new_keys.append((msg[i], int(msg[i+1])))

        new_keys = set(new_keys)

        keys = PEER_LIST.keys()

        common = new_keys & keys

        to_remove = keys - common
        to_add = new_keys - common

        for key in to_remove:
            PEER_LIST.pop(key)

        for key in to_add:
            PEER_LIST[key] = 1

def go_offline(a , b):
    MANAGER_SOCK.sendall("QUIT".encode())
    MANAGER_SOCK.close()
    print("")
    exit(0)
signal.signal(signal.SIGINT, go_offline)



def man_comm():
    while True:
        # Handle messages from manager.
        msg = str(MANAGER_SOCK.recv(BUFF_SIZE).decode())


        if msg == "":
            continue

        isQuit = False
        # If message is ping
        if msg == "PING":
            if isQuit:
                MANAGER_SOCK.sendall("QUIT".encode())
            else:
                MANAGER_SOCK.sendall("OK".encode())
            
            continue
        
        # Else update peerlist3
        t = Thread(target=update_peers, args=[msg])
        t.run()
t = Thread(target=man_comm)
t.start()

def peer_comm():
    while True:
        PEER_SOCK.listen()
        conn, addr = PEER_SOCK.accept()

        msg = conn.recv(BUFF_SIZE).decode().strip()

        if msg == "GET":
            reply = ""
            for path in FILES:
                f = open(path)
                x = len(f.readlines())
                f.close()
                reply += path
                reply += " "
                reply += str(x)
                reply += " "
            conn.sendall(reply.encode())
            conn.close()
            continue

        msg = msg.split(" ")

        if msg[0] == "DOWN":
            path = msg[1]
            chunk_s = int(msg[2])
            chunk_e = int(msg[3])

            f = open(path)

            x = f.readlines()

            if chunk_e > len(x):
                chunk_e = len(x)

            for i in range(chunk_s, chunk_e):
                conn.sendall(x[i].encode())
                t = conn.recv(BUFF_SIZE).decode()
                if int(t) == len(x[i]):
                    # print(f"sent line {i}")
                    continue
                else:
                    conn.sendall(x[i].encode())
                    t = conn.recv(BUFF_SIZE).decode()

            conn.sendall("DONE".encode())

            




c = Thread(target=peer_comm)
c.start()

def broadcast_req():
    key_set = set(PEER_LIST.keys()) - set(PEER_SOCK_ADDR)
    key_set.remove(PEER_SOCK_ADDR)
    print(f"------------------------- Fetching available files -------------------------------")
    for key in key_set:
        conn = socket(AF_INET, SOCK_STREAM)
        conn.connect(key)
        conn.sendall("GET".encode())
        reply = conn.recv(BUFF_SIZE).decode()
        pth = reply.strip().split(" ")
        for i in range(0, len(pth), 2):
            file = pth[i]
            
            try:
                FILE_SIZE[file] = int(pth[i+1])
            except:
                pass

            if  file != '':
                if FILE_PEER.get(file, -1) == -1 :
                    FILE_PEER[file] = []
                    FILE_PEER[file].append(key)
                else:
                    FILE_PEER[file].append(key)
        conn.close()

    print(" ")
    print(FILE_PEER)

    print("-----------------------------------------------------------------------------------")


def download(peer, path, chunk):
    revcd_chunk = ""

    conn = socket(AF_INET, SOCK_STREAM)
    conn.connect(peer)
    conn.sendall(f"DOWN {path} {chunk[0]} {chunk[1]}".encode())

    xi = conn.recv(BUFF_SIZE).decode().strip()
    conn.sendall(f"{len(xi)}".encode())
    # print(f"downloading chunk {chunk} from {peer}")
    # print(xi)
    success = True
    xi_prev = xi
    while xi != "DONE":
        try:
            conn.settimeout(50)
            xi = conn.recv(BUFF_SIZE).decode()
            # print(xi)
            conn.sendall(f"{len(xi)}".encode())
        except:
            print(f"Connection with peer {peer} close during download.... Downloading from other peers")
            success = False
            break
        
        xi_prev = xi
        if xi == "DONE":
            break
        else:
            revcd_chunk += xi

    if REDOWNLOADING:
        print("Too many disconnects ... Downloading failed")

    if success:
        Fragments_LOCK.acquire()
        Fragments.append((revcd_chunk, chunk))
        Fragments_LOCK.release()
    else:
        TO_REDOWNLOAD[peer] = (path, chunk)


def pdownload(peers, path):
    Fragments.clear()
    TO_REDOWNLOAD.clear()
    Thrd_pool = []
    start = 0
    step = math.ceil(FILE_SIZE[path]/len(peers))
    prev = start

    for peer in peers:
        t = Thread(target=download(peer, path, (prev, prev+step)))
        t.start()
        prev += step

    for Thrd in Thrd_pool:
        Thrd.join()

    if set(TO_REDOWNLOAD.keys()) != {}:
        REDOWNLOADING = True
        disc_peers = set(TO_REDOWNLOAD.keys())
        live_peers = set(peers).difference(disc_peers)

        i = 0
        j = 0
        while j < len(disc_peers):
            download(live_peers[i], path, TO_REDOWNLOAD[disc_peers[j][0]])
            i = (i+1)%len(live_peers)
            j+=1

    
def combine_chunks(dest):
    Frag = sorted(Fragments, key= lambda x: x[1][0])
    path = str(PEER_SOCK.getsockname())
    f = open(dest, "w")
    for piece in Frag:
        f.write(piece[0])
    

PROMPT = """
Enter an action:
    1. ADD -> Add FILE to list of available files
    2. GET -> Fetch available files from Peers.
    3. DOWN -> Download a file
    4. QUIT -> Exit peer
 """

def get_input():
    user_input = input(PROMPT)
    if user_input == "ADD":
        path = input("File Path: ")
        FILES.append(path)

    if user_input == "GET":
        print("File_path : peers")
        broadcast_req()

    if user_input == "DOWN":
        if set(FILE_PEER.keys()) != {}:
            path = input("Which file? Provide file path: ")
            dest = input("Where to store downloaded file? : ")
            if path not in FILE_PEER.keys():
                print("Invalid path. Type GET for FILE path : peers")
            else:
                peers = FILE_PEER[path]
                pdownload(peers, path)
                combine_chunks(dest)
        else:
            broadcast_req()
            path = input("Which file? Provide file path: ")
            dest = input("Where to store downloaded file? : ")

            if path not in FILE_PEER.keys():
                print("Invalid path. Type GET for FILE path : peers")
            else:
                peers = FILE_PEER[path]
                pdownload(peers, path)
                combine_chunks(dest)

    if user_input == "QUIT":
        go_offline(1,1)

    get_input()

get_input()