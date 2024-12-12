import socket
import threading
import json
import time
import hashlib
import os
import sys

PIECE_SIZE = 512000 #kB

# Danh sách lưu các thread
threads = []

class Peer:
    def __init__(self):
        self.tracker_host = "0.0.0.0"
        self.tracker_port = 8000
        self.server_port = 8002
        self.files = {}  # {(hash_code, file_name, size): [(index_piece, data), ...], ...}
        self.lock_var_files = threading.Lock()
        self.lock_repository = threading.Lock()
        self.tracker_sock = None

    def server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("0.0.0.0", self.server_port))
            s.listen(4) #giới hạn 4 client
            # print(f"Client server running on {self.host}:{self.port}")

            #! multi-upload    
            while True:
                conn, addr = s.accept()
                threading.Thread(target=self.handle_client, args=(conn, addr), daemon=False).start()
 
    def client(self):
        self.connect_to_tracker()
        
        while True:
            command_input = input("Client 2>>").strip()
            command = command_input.split(" ")
            if command[0] == "post":
                self.tracker_sock.sendall(command[0].encode())

                #gửi file json
                for path_file in command[1:]:
                    # kiểm tra có sẵn file không 
                    if not os.path.exists(path_file):
                        print(f"File {path_file} does not exist")
                        continue
                    self.post_file(path_file)
                #lệnh dừng
                while True:            
                    try:
                        stop_signal = {}
                        self.tracker_sock.sendall(json.dumps(stop_signal).encode().ljust(PIECE_SIZE, b"\x00"))
                        response = str(self.tracker_sock.recv(128).rstrip(b"\x00").decode())
                        print(response)
                        if "Finish" in response:
                            break
                    except (socket.timeout, ConnectionResetError) as e:
                        print(f"Lỗi kết nối: {e}")
                        break
            elif command[0] == "ping":
                self.tracker_sock.sendall(command_input.encode())

                while True:
                    response = str(self.tracker_sock.recv(128).rstrip(b"\x00").decode())
                    print(response)
                    if "Finish" in response:
                        break
            elif command[0] == "connectTracker":
                self.connect_to_tracker()
            elif command[0] == "fetch":
                self.tracker_sock.sendall(command_input.encode())
                # nhận file json từ tracker
                temp = json.loads(self.tracker_sock.recv(PIECE_SIZE).rstrip(b"\x00").decode())
                response = {eval(key): value for key, value in temp.items()}
                # nhận thông báo từ tracker
                print(self.tracker_sock.recv(128).rstrip(b"\x00").decode())

                if response == {}:
                    print("All file not available")
                else:
                    # download file từ peer khác
                    for file_info, pieces in response.items():
                        # chuyển đổi key từ string sang int
                        pieces = {int(key): value for key, value in pieces.items()}
                        
                        hash_code, file_name, size = file_info
                        self.files.setdefault(file_info, [])

                        # kiểm tra file đã tồn tại chưa, chỉ lấy piece thiếu trong json
                        # Thay thế đuôi file thành ".json"
                        file_name_json = os.path.splitext(file_name)[0] + ".json"
                        # Tạo đường dẫn đầy đủ tới file JSON
                        # full_path_json = ".\\repository\\" + file_name_json
                        full_path_json = os.path.join(r".\repository", file_name_json)
                        full_path_real_file = os.path.join(r".\repository", file_name)

                        # Kiểm tra file có tồn tại không
                        file_json = {"hash_code": hash_code, "file_name": file_name, "size": size, "pieces": []}
                        if os.path.exists(full_path_json):
                            try:
                                # Mở và đọc file JSON
                                with open(full_path_json, "r", encoding="utf-8") as file:
                                    file_json = json.load(file)

                                    # piece nào có rồi thì load du lieu vao files
                                    self.loadData(file_info, file_json["pieces"], full_path_real_file)
                            except json.JSONDecodeError as e:
                                print(f"Error decoding JSON in file {full_path_json}: {e}")
                            except Exception as e:
                                print(f"Error reading file in fetch command {full_path_json}: {e}")
                        # else: file chưa tồn tại sài mặc định

                        #! piece nào chưa có thì tạo multi-download
                        for index_piece, addr in pieces.items():
                            if index_piece not in file_json["pieces"]:
                                if addr != []:
                                    thread = threading.Thread(target=self.fetch_piece, args=(file_info, index_piece, tuple(addr)), daemon=False)
                                    thread.start()
                                    threads.append(thread)  # Lưu thread vào danh sách
                        
                        # Chờ tất cả các thread hoàn thành
                        for th in threads:
                            th.join()  # Đợi thread này kết thúc
                            
                        with self.lock_var_files:
                            # sort lại pieces
                            self.files[file_info] = self.sort_data(self.files[file_info])
                        
                        with self.lock_repository:
                            # ghi file chính
                            with open(full_path_real_file, "wb") as file:
                                for _, chunk in self.files[file_info].items():
                                    file.write(chunk)

                        with self.lock_repository:
                            # gom xong, update/create file json
                            try:
                                with open(full_path_json, "w", encoding="utf-8") as file:
                                    file_json["pieces"] = []
                                    for index_piece, _ in self.files[file_info].items():
                                        file_json["pieces"].append(index_piece)
                                    file.write(json.dumps(file_json, indent=4, ensure_ascii=False))

                                    #in ra thông báo file đầy đủ pieces có thể mở file
                                    total_pieces = (size + PIECE_SIZE - 1) // PIECE_SIZE
                                    total_pieces_available = len(file_json["pieces"])
                                    if total_pieces == total_pieces_available:
                                        print(f"File {file_name} is fully downloaded, you can open it")
                                    else:
                                        print(f"File {file_name} is not fully downloaded, you can't open it")
                            except json.JSONDecodeError as e:
                                print(f"Error decoding JSON in file {full_path_json}: {e}")
                            except Exception as e:
                                print(f"Error reading file in update/create command {full_path_json}: {e}")
            elif command[0] == "discover":
                self.tracker_sock.sendall(command[0].encode())
                try:
                    json_content = json.loads(self.tracker_sock.recv(PIECE_SIZE).rstrip(b"\x00").decode())
                except json.JSONDecodeError as e:
                    print(f"Lỗi giải mã JSON in discover: {e}")

                if json_content == []:  # Nếu dữ liệu rỗng 
                    print("Không có file nào được chia sẻ")
                else:
                    print(json.dumps(json_content, indent=4, ensure_ascii=False))  # indent để format đẹp
                print(self.tracker_sock.recv(128).rstrip(b"\x00").decode())
            elif command[0] == "end":
                self.tracker_sock.close()
                sys.exit() # safely exit the program
                break
            else:
                print("Unknown command")

    def loadData(self, file_info, pieces_valid, path_file):
        i = 0
        with self.lock_var_files:
            with open(path_file, "rb") as f:
                while True:
                    chunk = f.read(PIECE_SIZE)
                    data = (pieces_valid[i], chunk)
                    self.files[file_info].extend([data])
                    i += 1

    def sort_data(self, data):
        return dict(sorted(data, key=lambda x: x[0]))

    def fetch_piece(self, file_info, index_piece, addr):
        print(f"Begin fetching piece {index_piece} from {addr} by thead {threading.current_thread().name}")

        # Tạo socket mới để kết nối với peer khác
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(addr)
            s.sendall(f"{file_info[1]} {index_piece}".encode().ljust(128, b"\x00")) #gửi file_name và index_piece
            # Nhận dữ liệu từ peer khác

            response = s.recv(PIECE_SIZE).rstrip(b"\x00")
            data = (index_piece, response)
            with self.lock_var_files:    
                # Lưu dữ liệu vào files
                self.files[file_info].extend([data])
            s.close()
        print(f"Finish fetching piece {index_piece} from {addr} by thead {threading.current_thread().name}")
            
    def handle_client(self, conn, addr):
        with conn:
            print(f"\n Has been connected with {addr}")
            while True:
                data = conn.recv(128).rstrip(b"\x00")
                if not data:
                    break
                command = data.decode().split(" ") # file_name index_piece 
                print(f"Begin sending piece {command[1]} to {addr} by thread {threading.current_thread().name}")
                
                full_path_real_file = os.path.join(r".\repository", command[0])
                full_path_json = os.path.splitext(full_path_real_file)[0] + ".json"
                # kieu tra file co ton tai ko
                if not (os.path.exists(full_path_real_file) or os.path.exists(full_path_json)):
                    continue

                with self.lock_repository:    
                    #đọc file json lấy index của piece
                    with open(full_path_json, "r", encoding="utf-8") as file:
                        file_json = json.load(file)
                        index_piece = file_json["pieces"].index(int(command[1]))
                    
                with self.lock_repository:    
                    #gửi dữ liệu cho peer đang yêu cầu
                    with open(full_path_real_file, "rb") as file:
                        file.seek(index_piece * PIECE_SIZE)
                        data = file.read(PIECE_SIZE)
                        conn.sendall(data)
                print(f"Finish sending piece {command[1]} to {addr} by thread {threading.current_thread().name}")

    def connect_to_tracker(self):
        while True:
            try:
                self.tracker_sock = socket.create_connection((self.tracker_host, self.tracker_port), timeout=10)
                print(f"Connected to tracker on {self.tracker_host}:{self.tracker_port}")
                self.tracker_sock.sendall(f"{self.server_port}".encode())
                break
            except socket.timeout:
                print(f"Connection to tracker timed out 10s")
            except socket.error as e:
                print(f"Failed to connect to tracker: {e}")
                self.tracker_sock = None

    def generate_time_hash(self):
        # Lấy thời gian hiện tại (tính bằng số giây từ epoch)
        current_time = str(time.time())
        # Tạo hash bằng SHA-256
        hash_object = hashlib.sha256(current_time.encode())
        # Trả về chuỗi hash (dạng hexdigest)
        return hash_object.hexdigest()

    def post_file(self, path_file):
        #chuyen file sang json
        path_file_json = os.path.splitext(path_file)[0] + ".json"
        if not os.path.exists(path_file_json):
            # Tạo file JSON, coi như là file mới (full piece)
            try:
                with open(path_file_json, "w") as file:
                    size = os.path.getsize(path_file)
                    file_indices = [x for x in range(1, ((size + PIECE_SIZE - 1) // PIECE_SIZE) + 1)]
                    file.write(json.dumps({"hash_code": self.generate_time_hash(), "file_name": os.path.basename(path_file), "size": size, "pieces": file_indices}, indent=4))    
            except Exception as e:
                print(f"Error reading file in post command {path_file}: {e}")
        #file đã tồn tại
        try:
            # Đọc file JSON
            with open(os.path.join(path_file_json), "r") as file:
                json_data = json.load(file)
                # kiem tra file json
                if "file_name" not in json_data or json_data["file_name"] == "":
                    json_data["file_name"] = path_file
                if "hash_code" not in json_data or json_data["hash_code"] == "":
                    json_data["hash_code"] = self.generate_time_hash()
                if "size" not in json_data or json_data["size"] == "":
                    json_data["size"] = size
                if "pieces" not in json_data or json_data["pieces"] == "":
                    json_data["pieces"] = file_indices

            # Chuyển đổi JSON thành bytes
            data_bytes = json.dumps(json_data).encode()
            data_bytes = data_bytes.ljust(PIECE_SIZE, b"\x00")  # Pad with null bytes to reach n bytes
            self.tracker_sock.sendall(data_bytes)
            print(f"Sent file {path_file}")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON in file {path_file}: {e}")
        except Exception as e:
            print(f"Error reading file in post command {path_file}: {e}")

    def start(self):
        threading.Thread(target=self.server, daemon=True).start()
        self.client()

if __name__ == "__main__":
    peer = Peer()
    peer.start()
