# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0.

import unittest
import os
from tempfile import NamedTemporaryFile
from test import NativeResourceTest
from concurrent.futures import Future

from awscrt.http import HttpHeaders, HttpRequest
from awscrt.s3 import S3Client, S3RequestType
from awscrt.io import ClientBootstrap, ClientTlsContext, DefaultHostResolver, EventLoopGroup, TlsConnectionOptions, TlsContextOptions, init_logging, LogLevel
from awscrt.auth import AwsCredentialsProvider


def s3_client_new(secure, region, part_size=0):

    event_loop_group = EventLoopGroup()
    host_resolver = DefaultHostResolver(event_loop_group)
    bootstrap = ClientBootstrap(event_loop_group, host_resolver)
    credential_provider = AwsCredentialsProvider.new_default_chain(bootstrap)
    tls_option = None
    if secure:
        opt = TlsContextOptions()
        ctx = ClientTlsContext(opt)
        tls_option = TlsConnectionOptions(ctx)

    s3_client = S3Client(
        bootstrap=bootstrap,
        region=region,
        credential_provider=credential_provider,
        tls_connection_options=tls_option,
        part_size=part_size)

    return s3_client


class FakeReadStream(object):
    def __init__(self, read_future):
        self._future = read_future
        pass

    def read(self, length):
        fake_string = "x" * length
        fake_data = bytes(fake_string, 'utf-8')
        if not self._future.done():
            self._future.set_result(None)
        return fake_data


class S3ClientTest(NativeResourceTest):

    def setUp(self):
        self.region = "us-west-2"
        self.timeout = 10  # seconds

    def test_sanity(self):
        s3_client = s3_client_new(False, self.region)
        self.assertIsNotNone(s3_client)

    def test_sanity_secure(self):
        s3_client = s3_client_new(True, self.region)
        self.assertIsNotNone(s3_client)

    def test_wait_shutdown(self):
        s3_client = s3_client_new(False, self.region)
        self.assertIsNotNone(s3_client)

        shutdown_event = s3_client.shutdown_event
        del s3_client
        self.assertTrue(shutdown_event.wait(self.timeout))


class S3RequestTest(NativeResourceTest):
    def setUp(self):
        self.get_test_object_path = "/get_object_test_10MB.txt"
        self.put_test_object_path = "/put_object_test_py_10MB.txt"
        self.region = "us-west-2"
        self.bucket_name = "aws-crt-canary-bucket"
        self.timeout = 100  # seconds
        self.num_threads = 0

        self.response_headers = None
        self.response_status_code = None
        self.received_body_len = 0
        self.transferred_len = 0
        self.data_len = 0
        self.progress_invoked = 0

        self.put_body_stream = None

    def _build_endpoint_string(self, region, bucket_name):
        return bucket_name + ".s3." + region + ".amazonaws.com"

    def _get_object_request(self, object_path):
        headers = HttpHeaders([("host", self._build_endpoint_string(self.region, self.bucket_name))])
        request = HttpRequest("GET", object_path, headers)
        return request

    def _put_object_request(self, file_name):
        self.put_body_stream = open(file_name, "r+b")
        file_stats = os.stat(file_name)
        self.data_len = file_stats.st_size
        headers = HttpHeaders([("host", self._build_endpoint_string(self.region, self.bucket_name)),
                               ("Content-Type", "text/plain"), ("Content-Length", str(self.data_len))])
        request = HttpRequest("PUT", self.put_test_object_path, headers, self.put_body_stream)
        return request

    def _on_request_headers(self, status_code, headers, **kargs):
        self.response_status_code = status_code
        self.response_headers = headers

    def _on_request_body(self, chunk, offset, **kargs):
        self.received_body_len = self.received_body_len + len(chunk)

    def _on_progress(self, progress):
        self.transferred_len += progress

    def _validate_successful_get_response(self, put_object):
        self.assertEqual(self.response_status_code, 200, "status code is not 200")
        headers = HttpHeaders(self.response_headers)
        self.assertIsNone(headers.get("Content-Range"))
        body_length = headers.get("Content-Length")
        if not put_object:
            self.assertIsNotNone(body_length, "Content-Length is missing from headers")
        if body_length:
            self.assertEqual(
                int(body_length),
                self.received_body_len,
                "Received body length does not match the Content-Length header")

    def _test_s3_put_get_object(self, request, request_type, exception_name=None):
        s3_client = s3_client_new(False, self.region, 5 * 1024 * 1024)
        init_logging(LogLevel.Debug, "stderr")
        s3_request = s3_client.make_request(
            request=request,
            type=request_type,
            on_headers=self._on_request_headers,
            on_body=self._on_request_body)
        finished_future = s3_request.finished_future
        try:
            finished_future.result(self.timeout)
        except Exception as e:
            self.assertEqual(e.name, exception_name)
        else:
            self._validate_successful_get_response(request_type is S3RequestType.PUT_OBJECT)
        shutdown_event = s3_request.shutdown_event
        del s3_request
        self.assertTrue(shutdown_event.wait(self.timeout))

    def test_get_object(self):
        request = self._get_object_request(self.get_test_object_path)
        self._test_s3_put_get_object(request, S3RequestType.GET_OBJECT)

    def test_put_object(self):
        request = self._put_object_request("test/resources/s3_put_object.txt")
        self._test_s3_put_get_object(request, S3RequestType.PUT_OBJECT)
        self.put_body_stream.close()

    def test_get_object_file_object(self):
        request = self._get_object_request(self.get_test_object_path)
        request_type = S3RequestType.GET_OBJECT
        s3_client = s3_client_new(False, self.region, 5 * 1024 * 1024)
        with NamedTemporaryFile(mode="w", delete=False) as file:
            file.close()
            s3_request = s3_client.make_request(
                request=request,
                type=request_type,
                recv_filepath=file.name,
                on_headers=self._on_request_headers,
                on_progress=self._on_progress)
            finished_future = s3_request.finished_future
            finished_future.result(self.timeout)

            # Result check
            self.data_len = int(HttpHeaders(self.response_headers).get("Content-Length"))
            file_stats = os.stat(file.name)
            file_len = file_stats.st_size
            self.assertEqual(
                file_len,
                self.transferred_len,
                "the length of written file does not match the transferred length reported")
            self.assertEqual(
                self.data_len,
                self.transferred_len,
                "the transferred length reported does not match the content-length header")
            self.assertEqual(self.response_status_code, 200, "status code is not 200")
            shutdown_event = s3_request.shutdown_event
            del s3_request
            self.assertTrue(shutdown_event.wait(self.timeout))
            # TODO verify the content of written file
            os.remove(file.name)

    def test_put_object_file_object(self):
        request = self._put_object_request("test/resources/s3_put_object.txt")
        request_type = S3RequestType.PUT_OBJECT
        # close the stream, to test if the C FILE pointer as the input stream working well.
        self.put_body_stream.close()
        s3_client = s3_client_new(False, self.region, 5 * 1024 * 1024)
        s3_request = s3_client.make_request(
            request=request,
            type=request_type,
            send_filepath="test/resources/s3_put_object.txt",
            on_headers=self._on_request_headers,
            on_progress=self._on_progress)
        finished_future = s3_request.finished_future
        finished_future.result(self.timeout)

        # check result
        self.assertEqual(
            self.data_len,
            self.transferred_len,
            "the transferred length reported does not match body we sent")
        self._validate_successful_get_response(request_type is S3RequestType.PUT_OBJECT)
        shutdown_event = s3_request.shutdown_event
        del s3_request
        self.assertTrue(shutdown_event.wait(self.timeout))

    def _on_progress_cancel_after_first_chunk(self, progress):
        self.transferred_len += progress
        self.progress_invoked += 1
        self.s3_request.cancel()

    def test_multipart_get_object_cancel(self):
        # a 5 GB file
        request = self._get_object_request("/crt-canary-obj-single-part-9223372036854775807")
        s3_client = s3_client_new(False, self.region, 5 * 1024 * 1024)
        with NamedTemporaryFile(mode="w", delete=False) as file:
            file.close()
            self.s3_request = s3_client.make_request(
                request=request,
                recv_filepath=file.name,
                type=S3RequestType.GET_OBJECT,
                on_headers=self._on_request_headers,
                on_progress=self._on_progress_cancel_after_first_chunk)
            finished_future = self.s3_request.finished_future
            try:
                finished_future.result(self.timeout)
            except Exception as e:
                self.assertEqual(e.name, "AWS_ERROR_S3_CANCELED")

            # Result check
            self.data_len = int(HttpHeaders(self.response_headers).get("Content-Length"))
            self.assertLess(
                self.transferred_len,
                self.data_len,
                "the cancel failed to block all the following body")

            # The on_finish callback may invoke the progress
            self.assertLessEqual(self.progress_invoked, 2)
            shutdown_event = self.s3_request.shutdown_event
            del self.s3_request
            self.assertTrue(shutdown_event.wait(self.timeout))
            os.remove(file.name)

    def test_get_object_quick_cancel(self):
        # a 5 GB file
        request = self._get_object_request("/crt-canary-obj-single-part-9223372036854775807")
        s3_client = s3_client_new(False, self.region, 5 * 1024 * 1024)
        with NamedTemporaryFile(mode="w", delete=False) as file:
            file.close()
            s3_request = s3_client.make_request(
                request=request,
                recv_filepath=file.name,
                type=S3RequestType.GET_OBJECT,
                on_headers=self._on_request_headers,
                on_progress=self._on_progress)
            s3_request.cancel()
            finished_future = s3_request.finished_future
            try:
                finished_future.result(self.timeout)
            except Exception as e:
                self.assertEqual(e.name, "AWS_ERROR_S3_CANCELED")

            shutdown_event = s3_request.shutdown_event
            del s3_request
            self.assertTrue(shutdown_event.wait(self.timeout))
            os.remove(file.name)

    def _put_object_cancel_helper(self, cancel_after_read):
        read_futrue = Future()
        put_body_stream = FakeReadStream(read_futrue)
        data_len = 10 * 1024 * 1024 * 1024  # some fake length
        headers = HttpHeaders([("host", self._build_endpoint_string(self.region, self.bucket_name)),
                               ("Content-Type", "text/plain"), ("Content-Length", str(data_len))])
        http_request = HttpRequest("PUT", "/cancelled_request", headers, put_body_stream)
        s3_client = s3_client_new(False, self.region, 5 * 1024 * 1024)
        s3_request = s3_client.make_request(
            request=http_request,
            type=S3RequestType.PUT_OBJECT,
            on_headers=self._on_request_headers)

        if cancel_after_read:
            read_futrue.result(self.timeout)

        s3_request.cancel()
        finished_future = s3_request.finished_future
        try:
            finished_future.result(self.timeout)
        except Exception as e:
            self.assertEqual(e.name, "AWS_ERROR_S3_CANCELED")

        shutdown_event = s3_request.shutdown_event
        del s3_request
        self.assertTrue(shutdown_event.wait(self.timeout))

        # TODO If CLI installed, run the following command to ensure the cancel succeed.
        # aws s3api list-multipart-uploads --bucket aws-crt-canary-bucket --prefix 'cancelled_request'
        # Nothing should printout

    def test_multipart_put_object_cancel(self):
        return self._put_object_cancel_helper(True)

    def test_put_object_quick_cancel(self):
        return self._put_object_cancel_helper(False)

    def test_multipart_upload_with_invalid_request(self):
        request = self._put_object_request("test/resources/s3_put_object.txt")
        request.headers.set("Content-MD5", "something")
        self._test_s3_put_get_object(request, S3RequestType.PUT_OBJECT, "AWS_ERROR_S3_INVALID_RESPONSE_STATUS")
        self.put_body_stream.close()

    def test_multipart_upload_with_bad_region(self):
        request = self._put_object_request("test/resources/s3_put_object.txt")
        self.region = "something"
        self._test_s3_put_get_object(request, S3RequestType.PUT_OBJECT, "AWS_ERROR_S3_INVALID_RESPONSE_STATUS")
        self.put_body_stream.close()

    def test_multipart_download_with_invalid_region(self):
        request = self._get_object_request(self.get_test_object_path)
        self.region = "us-west-1"
        self._test_s3_put_get_object(request, S3RequestType.GET_OBJECT)

    def test_multipart_upload_with_invalid_file_path(self):
        request = self._put_object_request("test/resources/s3_put_object.txt")
        request_type = S3RequestType.PUT_OBJECT
        # close the stream, to test if the C FILE pointer as the input stream working well.
        self.put_body_stream.close()
        s3_client = s3_client_new(False, self.region, 10 * 1024 * 1024)
        s3_request = s3_client.make_request(
            request=request,
            type=request_type,
            send_filepath="test/resources/",  # invalid path
            on_headers=self._on_request_headers,
            on_progress=self._on_progress)
        finished_future = s3_request.finished_future
        # Finish future should result in error. Failed read from input stream
        finished_future.result(self.timeout)

        # check result
        self.assertEqual(
            self.data_len,
            self.transferred_len,
            "the transferred length reported does not match body we sent")
        self._validate_successful_get_response(request_type is S3RequestType.PUT_OBJECT)
        shutdown_event = s3_request.shutdown_event
        del s3_request
        self.assertTrue(shutdown_event.wait(self.timeout))


if __name__ == '__main__':
    unittest.main()
