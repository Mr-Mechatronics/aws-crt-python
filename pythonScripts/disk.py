import awscrt.s3
from awscrt.io import ClientBootstrap, DefaultHostResolver, EventLoopGroup, LogLevel
from awscrt.auth import AwsCredentialsProvider
import os
from concurrent.futures import as_completed
import time

# Configurations
REGION = "us-east-1"
BUCKET = "waqar-s3-test-east-1"
SAMPLE_CHECKPOINT_LOCATION = "./data"
NETWORK_INTERFACE_NAMES = ["enp71s0","enp105s0", "enp173s0", "enp139s0"]
TARGET_THROUGHPUT_GBPS = 400

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
    memory_limit=32*1024*1024*1024,
    network_interface_names=NETWORK_INTERFACE_NAMES)


def upload_directory(directory_path, prefix=""):
    total_size = 0
    start_time = time.time()

    requests = []
    for root, _, files in os.walk(directory_path):
        for file in files:
            file_path = os.path.join(root, file)
            relative_path = os.path.relpath(file_path, directory_path)
            object_key = os.path.join(prefix, relative_path).replace("\\", "/")
            total_size += os.path.getsize(file_path)
            headers = awscrt.http.HttpHeaders()
            headers.add('Host', BUCKET + ".s3." + REGION + ".amazonaws.com")
            headers.add('Content-Length', str(os.path.getsize(file_path)))
            headers.add('Content-Type', 'application/octet-stream')
            request = awscrt.http.HttpRequest(
                "PUT", f"/{object_key}", headers, None)
            requests.append(s3_client.make_request(
                type=awscrt.s3.S3RequestType.PUT_OBJECT,
                request=request,
                send_filepath=file_path))

    request_futures = [r.finished_future for r in requests]
    for finished_future in as_completed(request_futures):
        finished_future.result()

    end_time = time.time()
    duration = end_time - start_time
    throughput = (total_size / duration) / (1024 * 1024 * 1024)  # GB/s
    print(
        f"Directory {directory_path} upload complete. Total size: {total_size / (1024 ** 3):.2f} GB, Duration: {duration:.2f} s, Throughput: {throughput*8:.2f} Gb/s"
    )


upload_directory(SAMPLE_CHECKPOINT_LOCATION, "test-s3-multinic-disk/")
