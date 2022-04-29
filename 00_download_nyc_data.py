import time
import boto3
import os
from botocore import UNSIGNED
from botocore.client import Config
from botocore.exceptions import ResponseStreamingError

s3 = boto3.resource("s3", config=Config(signature_version=UNSIGNED))


def download_s3_folder(bucket_name, s3_folder, local_dir=None):
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=s3_folder):
        target = (
            obj.key
            if local_dir is None
            else os.path.join(local_dir, os.path.relpath(obj.key, s3_folder))
        )
        if os.path.exists(target):
            print(f"skipping {target}")
            continue
        if not os.path.exists(os.path.dirname(target)):
            os.makedirs(os.path.dirname(target))
        if obj.key[-1] == "/":
            continue
        print(f"downloading {target}")
        try:
            bucket.download_file(obj.key, target)
        except ResponseStreamingError:
            time.sleep(60)
            bucket.download_file(obj.key, target)
        print(f"downloaded {target}")

if __name__ == '__main__':
    print("BEGIN DOWNLOAD")
    download_s3_folder("ursa-labs-taxi-data", "", "nyc-taxi")
    print("DONE DOWNLOAD!!!")
