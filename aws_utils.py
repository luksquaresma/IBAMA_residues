import boto3, os, sys
import threading


class S3ProgressPercentage(object):

    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()


# send files to AWS S3
def send_to_s3(source_file_path, directory_uri, termianl_progress=False):

    # s3 string manipulations 
    s3_prefix = "s3://"
    bucket = directory_uri.removeprefix("s3://").split("/")[0]
    directory = directory_uri.removeprefix(f"{s3_prefix}{bucket}/")
    
    # filename string manipulations
    file_name = source_file_path.split("/")[-1]

    # actual upload
    s3 = boto3.client('s3')
    with open(source_file_path, "rb") as f:
        s3.upload_fileobj(
            Fileobj = f,
            Bucket = bucket,
            Key = f"{directory}{file_name}",
            Callback = S3ProgressPercentage(file_name) if termianl_progress else None, 
            ExtraArgs = None,
            Config = None
            )
