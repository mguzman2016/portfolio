import os
import csv

def get_file_handle(file_location, columns):
    file = open(file_location, "a", encoding="utf-8", newline="")
    writer = csv.DictWriter(
            file,
            fieldnames=columns,
            extrasaction="ignore",
            quoting=csv.QUOTE_ALL,
            lineterminator="\n",
            escapechar='\\'
    )
    writer.writeheader()
    file.flush()
    return (writer, file)

def stream_file_lines(file_location):
    with open(file_location, "r", encoding="utf-8", newline="") as file:
        reader = csv.reader(file)
        for row in reader:
            if row:
                yield row[0]

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