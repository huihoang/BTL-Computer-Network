import os, time, hashlib, json

def split_file_by_pieces(source_file, piece_size, arr1, output_file1, output_file2):
    try:
        if not os.path.exists(source_file):
            print(f"File {source_file} không tồn tại!")
            return
 
        file_size = os.path.getsize(source_file)
        print(f"Kích thước file: {file_size} bytes")

    
        total_pieces = (file_size + piece_size - 1) // piece_size  # Tính số lượng pieces
        
        with open(source_file, "rb") as src, \
            open(output_file1, "wb") as out1, \
            open(output_file2, "wb") as out2:
            
            for index_piece in range(total_pieces):
                # Đọc dữ liệu của piece hiện tại
                data = src.read(piece_size)
                
                # Nếu index_piece (bắt đầu từ 0) thuộc nhóm arr1 thì ghi vào file1
                if (index_piece + 1) in arr1:  # index_piece + 1 để tính từ piece 1
                    out1.write(data)
                else:  # Ngược lại ghi vào file2
                    out2.write(data)

        hash_code = generate_time_hash()    
        with open(output_file11, "w") as file:
            json.dump(
                {
                    "hash_code": hash_code,
                    "file_name": source_file,
                    "size": os.path.getsize(source_file),
                    "pieces": arr1
                },
                file,
                indent=4  # Thêm tham số indent để tạo định dạng dễ đọc
            )

        arr2 = [x for x in range(1, total_pieces+1) if x not in arr1]
        with open(output_file22, "w") as file:
            json.dump(
                {
                    "hash_code": hash_code,
                    "file_name": source_file,
                    "size": os.path.getsize(source_file),
                    "pieces": arr2
                },
                file,
                indent=4  # Thêm tham số indent để tạo định dạng dễ đọc
            )

        print(f"Tách file thành công! Đã lưu vào '{output_file1}' và '{output_file2}'.")

    except Exception as e:
        print(f"Lỗi khi tách file: {e}")

def generate_time_hash():
    # Lấy thời gian hiện tại (tính bằng số giây từ epoch)
    current_time = str(time.time())
    # Tạo hash bằng SHA-256
    hash_object = hashlib.sha256(current_time.encode())
    # Trả về chuỗi hash (dạng hexdigest)
    return hash_object.hexdigest()

# Ví dụ sử dụng
source_file = "received_example.pdf"
piece_size = 512000  # Kích thước mỗi piece
arr1 = [1, 3, 5]  # Các pieces muốn ghi vào file 1
output_file1 = "file_1.pdf"
output_file11 = "file_1.json"
output_file2 = "file_2.pdf"
output_file22 = "file_2.json"

split_file_by_pieces(source_file, piece_size, arr1, output_file1, output_file2)
