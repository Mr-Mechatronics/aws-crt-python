import io

import awscrt.s3
from awscrt.io import ClientBootstrap, DefaultHostResolver, EventLoopGroup, LogLevel, init_logging
from awscrt.auth import AwsCredentialsProvider
import os
from concurrent.futures import as_completed
import time

# Configurations
init_logging(LogLevel.Error, "stdout")

REGION = "us-east-1"
BUCKET = "waqar-s3-test-east-1"
FILE_SIZE_GB = 30
NUM_FILES = 10
NETWORK_INTERFACE_NAMES = ["ens32","ens64", "ens96", "ens128"]
TARGET_THROUGHPUT_GBPS = 400
FILE_PATH = "../pythonScripts/30Gb"
# Initialization
event_loop_group = EventLoopGroup()
host_resolver = DefaultHostResolver(event_loop_group)
bootstrap = ClientBootstrap(event_loop_group, host_resolver)
credential_provider = AwsCredentialsProvider.new_default_chain(bootstrap)
signing_config = awscrt.s3.create_default_s3_signing_config(
    region=REGION,
    credential_provider=credential_provider)
s3_client = awscrt.s3.S3Client(
    bootstrap=bootstrap,
    region=REGION,
    signing_config=signing_config,
    throughput_target_gbps=TARGET_THROUGHPUT_GBPS,
    network_interface_names=NETWORK_INTERFACE_NAMES)

def memfile_path(fd):
  return f"/proc/{os.getpid()}/fd/{fd}"

def upload_files_from_ram(prefix=""):
    size = FILE_SIZE_GB * 1024 * 1024 * 1024
    files_path = []
    for i in range(NUM_FILES):
        fd = os.memfd_create(f"data{i}")
        path = memfile_path(fd)
        os.system(f"cp {FILE_PATH} {path}")
        files_path.append(path)

    print(f"created files: {len(files_path)}")
    total_size = 0
    start_time = time.time()
    requests = []
    headers = awscrt.http.HttpHeaders()
    headers.add('Host', BUCKET + ".s3." + REGION + ".amazonaws.com")
    headers.add('Content-Length', str(size))
    headers.add('Content-Type', 'application/octet-stream')

    for i in range(NUM_FILES):

        request = awscrt.http.HttpRequest(
            "PUT", f"/{prefix}data{i}", headers, None)
        requests.append(s3_client.make_request(
            type=awscrt.s3.S3RequestType.PUT_OBJECT,
            request=request,
            send_filepath=files_path[i]))
        total_size = total_size + size
    print("submitted")
    request_futures = [r.finished_future for r in requests]
    for finished_future in as_completed(request_futures):
        finished_future.result()

    end_time = time.time()
    duration = end_time - start_time
    throughput = (total_size / duration) / (1024 * 1024 * 1024)  # GB/s
    print(
        f"Ram upload complete. Total size: {total_size / (1024 ** 3):.2f} GB, Duration: {duration:.2f} s, Throughput: {throughput*8:.2f} Gb/s"
    )

upload_files_from_ram("test-s3-multinic-ram/")
