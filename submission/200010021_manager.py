import threading
import time
from socket import *
from threading import Thread

# List of active peers, key is addr tuple - value is socket
PEERLIST = dict()
PEER_LOCK = dict()

MANAGER_HOST = "localhost"
MANAGER_PORT = 23342
BUFF_SIZE = 4096

# create and bind socket
MANAGER_SOCK = socket(AF_INET, SOCK_STREAM)
MANAGER_SOCK.bind((MANAGER_HOST, MANAGER_PORT))

PEERLIST_TO_PEER = dict()


def broad_cast_list():

    Bcast_msg = ""

    for k in PEERLIST.copy():
        Bcast_msg += str(PEERLIST_TO_PEER[k][0])
        Bcast_msg += " "
        Bcast_msg += str(PEERLIST_TO_PEER[k][1])
        Bcast_msg += " "


    for k in PEERLIST.copy():
        try:
            PEER_LOCK[k].acquire()
            PEERLIST[k].sendall(Bcast_msg.encode())
            PEER_LOCK[k].release()
        except:
            pass
            

def handle_connection(conn: socket, addr):
    # if peer doesnot respond it is inactive.
    # if peer send quit message it is inactive.
    # periodically check if peer is active.

    while True:
        if PEERLIST.get(addr, -1) == -1:
            print(f"connection {addr} broke")
            broad_cast_list()
            break

        try:
            PEER_LOCK[addr].acquire()
            conn.sendall("PING".encode())
            conn.settimeout(0.1)
            msg = conn.recv(BUFF_SIZE).decode()
            PEER_LOCK[addr].release()
            if msg != "OK":
                PEERLIST.pop(addr)
                PEERLIST_TO_PEER.pop(addr)
        except:
            PEERLIST.pop(addr)
            PEERLIST_TO_PEER.pop(addr)

        


        time.sleep(0.1)

while True:
    # Accept connection from new peer.
    MANAGER_SOCK.listen()
    conn, addr = MANAGER_SOCK.accept()
    print(f"Peer connected {addr}")

    msg = conn.recv(BUFF_SIZE).decode().strip().split(" ")
    peer_host = msg[0]
    peer_port = int(msg[1])

    # Add new connection to peerlist
    PEERLIST[addr] = conn
    PEERLIST_TO_PEER[addr] = (peer_host, peer_port)
    PEER_LOCK[addr] = threading.Lock()

    # Handle connection in a thread
    t = Thread(target=handle_connection, args=(conn, addr))
    t.start()

    # broadcast peerlist
    broad_cast_list()
    


MANAGER_SOCK.close()



