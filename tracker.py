import socket
import threading
import json
import time
import hashlib
import random

PEER_TIMEOUT = 10  # Thời gian chờ ping trước khi lọc peer
PING_TIMEOUT = 3
PIECE_SIZE = 512000 # bytes

class TrackerServer:
    def __init__(self):
        # nhận mọi IP client với port 8000
        self.bind_ip  = "0.0.0.0"
        self.bind_port = 8000

        # kiểu dl là dictionary 
        self.peers = {}  # {(peer_address, port): [(hash_code, file_name, size), ...], ...}
        self.files = {}  # {(hash_code, file_name, size): {index_piece: [(peer1,port), ...]}, ...}
        self.lock = threading.Lock()

    def handle_client(self, conn, addr):
        print(f"\nNew connection: {addr}")
        with conn:
            while True:
                try:
                    data = conn.recv(128).decode()
                    if not data: # when the client closes, it will return an empty string '' and the connection will be closed
                        break
                    response = self.handle_command(data, addr, conn) # handle request from client
                    conn.sendall(response)
                except Exception as e:
                    print(f"Error with {e}")
                    break

    def initialize_peer(self, addr):
        with self.lock:
            if addr not in self.peers:
                self.peers[addr] = []  # Khởi tạo danh sách file cho peer mới

    def handle_command(self, data, addr, conn):
        command = data.strip().split(" ")
        if command[0] == "ping":
            if command[1] == "tracker":
                response = "\nTracker onl!"
                response = response.encode().ljust(128, b"\x00")
                conn.sendall(response)
            elif command[1] == "all":
                for peer in list(self.peers.keys()):  # Duyệt qua danh sách
                    if self.ping_peer(peer):
                        response = f"Peer{peer} onl!"
                    else:
                        response = f"Peer{peer} off!"
                    response = response.encode().ljust(128, b"\x00")
                    conn.sendall(response)
        elif command[0] == "post":
            while True:
                #nhận file json từ client
                try:
                    response = conn.recv(PIECE_SIZE).rstrip(b"\x00").decode()
                    json_content = json.loads(response)  # Parse JSON

                    #client bao ket thuc nhận file json
                    if json_content == {}:
                        break
                except Exception as e:
                    print(f"Lỗi nhận lệnh post: {e}")
                    break

                # Đọc nội dung file JSON
                hash_code = json_content.get("hash_code", "")  # Trả về giá trị hoặc chuỗi rỗng nếu không tồn tại
                file_name = json_content.get("file_name", "")
                pieces = json_content.get("pieces", [])
                size = json_content.get("size", 0)
                
                # từ chối nếu thiếu thông tin
                if size == 0 or file_name == "":
                    # conn.sendall("File .json thông tin thiếu!".encode())
                    continue
                if hash_code != "" and pieces == []:
                    # conn.sendall("File .json thông tin thiếu!".encode())
                    continue
                if hash_code == "": # file mới
                    hash_code = self.generate_time_hash()
                
                #tạo tuple
                newData = (hash_code, file_name, size)
                #tính số piece của file
                numPiece = (int(size) + PIECE_SIZE - 1) // PIECE_SIZE 
   
                with self.lock:
                    #update metainfo cho tracker
                    if newData not in self.peers[addr]:
                        self.peers[addr].append(newData)

                with self.lock:
                    if newData not in self.files:
                        #tạo mới và thêm data
                        for i in range(1, int(numPiece) + 1):
                            self.files.setdefault(newData, {})
                            self.files[newData].setdefault(i, [])
                            #thêm peer vào mảnh
                            if i in pieces:
                                self.files[newData][i].append(addr)
                    else:
                        for item in pieces:
                            if addr not in self.files[newData][item]:
                                self.files[newData][item].append(addr)

                #in ra thông báo
                self.print_peers_and_files()
        elif command[0] == "fetch":
            # tạo list các file cần fetch lưu vào command
            for i in range(len(command) - 1, 0, -1):
                for key in self.files.keys():
                    if str(key[0]) == str(command[i]) or (str(key[1]) == str(command[i])):
                        command[i] = key
                        break # find first match, lấy (hash_code,..)
                if not isinstance(command[i], tuple):
                    del command[i]

            #tạo gói dữ liệu
            dataFetch = {}
            #ko co file nao available
            if len(command) > 1:
                for key in command[1:]:
                    dataFetch.setdefault(key, {})
                    for index_piece, addr_clients in self.files[key].items():
                        if addr_clients:
                            dataFetch[key].setdefault(index_piece, random.choice(addr_clients))
                        else:
                            dataFetch[key].setdefault(index_piece, [])
            
            #chuyển đổi dữ liệu thành chuẩn json
            data_json_ready = {str(key): value for key, value in dataFetch.items()}
            # Chuyển đổi thành bytes
            data_bytes = json.dumps(data_json_ready).encode()  
            data_bytes = data_bytes.ljust(PIECE_SIZE, b"\x00")  # Đệm thêm null bytes cho đủ n bytes
            conn.sendall(data_bytes)
        elif command[0] == "discover":
            data_bytes = json.dumps(list(self.files.keys())) #chuyển về list vì ko chuyển trực tiếp từ dict sang json
            data_bytes = data_bytes.encode().ljust(PIECE_SIZE, b"\x00")
            conn.sendall(data_bytes)
        else:
            command[0] = command[0] + " (not found) "
        return ("Finish ".encode() + command[0].encode() + " command!".encode()).ljust(128, b"\x00")

    def print_peers_and_files(self):
        print("\n-------------------- Danh sách các peer đang theo dõi --------------------")
        for (peer_address, port), files in self.peers.items():
            print(f"- Peer: {peer_address}:{port}")
            for hash_code, file_name, size in files:
                print(f"  + File: {file_name} | Hash: {hash_code} | Size: {size} MB")

        print("\n-------------------- Danh sách các pieces sẵn sàng chia sẻ --------------------")
        for (hash_code, file_name, size), pieces in self.files.items():
            print(f"- File: {file_name} | Hash: {hash_code} | Size: {size} MB")
            for index_piece, peer_list in pieces.items():
                print(f"  + Piece {index_piece}:")
                for peer_address, port in peer_list:
                    print(f"    - Peer: {peer_address}:{port}")

    def generate_time_hash(self):
        # Lấy thời gian hiện tại (tính bằng số giây từ epoch)
        current_time = str(time.time())
        # Tạo hash bằng SHA-256
        hash_object = hashlib.sha256(current_time.encode())
        # Trả về chuỗi hash (dạng hexdigest)
        return hash_object.hexdigest()

    def ping_peer(self, peer):
        try:
            # Sử dụng kết nối nhanh để gửi ping tới peer, tự đóng
            with socket.create_connection((peer[0], peer[1]), timeout=PING_TIMEOUT):
                return True
        except (socket.timeout, ConnectionRefusedError):
            return False
    
    def cleanup_peers(self):
        while True:
            time.sleep(PEER_TIMEOUT)
            if not self.peers:
                continue
            print("\n\n-------------------- Checking peer --------------------")
            with self.lock:
                for peer in list(self.peers.keys()):  # Duyệt qua danh sách
                    if not self.ping_peer(peer):
                        print(f"Peer {peer} offline.")
                        self.remove_peer_from_files(peer)
                        del self.peers[peer]
                    else:
                        print(f"Peer {peer} online.")

    def remove_peer_from_files(self, peer):
        if not self.files:
            return

        to_delete = []
        for key, value in self.files.items():
            total = (key[2] + PIECE_SIZE - 1) // PIECE_SIZE
            count = 0
            for _, peers in value.items():
                if peer in peers:
                    peers.remove(peer)
                if not peers:
                    count += 1
            if count == total:
                to_delete.append(key)

        for key in to_delete:
            del self.files[key]

    def start(self):
        #deamon=true là ko đợi thread khi end chương trình
        threading.Thread(target=self.cleanup_peers, daemon=True).start()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.bind_ip, self.bind_port))
            s.listen(4) #giới hạn 4 client
            print(f"Tracker server listening on {self.bind_ip}:{self.bind_port}")
            while True:
                try:
                    conn, addr = s.accept()

                    addr_server_of_client = (addr[0], int(conn.recv(4).decode())) # Đọc 4 bytes đầu tiên để lấy port
                    self.initialize_peer((addr_server_of_client[0], addr_server_of_client[1]))

                    threading.Thread(target=self.handle_client, args=(conn, addr_server_of_client), daemon=True).start()
                except Exception as e:
                    print(f"Error accepting connection: {e}")

if __name__ == "__main__":
    tracker = TrackerServer()
    tracker.start()