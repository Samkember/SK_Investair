import os, sys
from s3_manager import S3Manager

bucket = "xtf-asx"
prefix = "20250522/"

def list_txt_files():
    s3 = S3Manager()
    txt_files = s3.list_files(bucket_name=bucket, folder=prefix)
    txt_files = [f for f in txt_files if f.endswith(".txt")]
    print(f"Filtered {len(txt_files)}.txt files.")
    
    count = 0
    for file in txt_files:
        raw_output = s3.get_file_bytes(bucket_name=bucket, key=file)
        decoded = raw_output.decode('utf-8')
        lines = decoded.splitlines()
            
        
        
        # count += 1
        # if count == 10:
        #     sys.exit(0)
    

if __name__ == "__main__":
    list_txt_files()
