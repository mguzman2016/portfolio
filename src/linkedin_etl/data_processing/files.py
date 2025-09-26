import os

def create_tmp_dir(tmp_directory):
    tmp_directory.mkdir(parents=True, exist_ok=True)

def delete_all_files(directory):
    files = os.listdir(directory)
    print("Files:", files)
    
    for file in files:
        file_path = os.path.join(directory, file)
        if os.path.isfile(file_path):
            os.remove(file_path)
            print(f"Deleted: {file_path}")
