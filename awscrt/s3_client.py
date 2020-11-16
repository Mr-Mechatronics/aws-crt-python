"""
S3 client
"""

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0.

import _awscrt
from concurrent.futures import Future
from awscrt import NativeResource
from awscrt.http import HttpRequest
from awscrt.io import ClientBootstrap, TlsConnectionOptions
from awscrt.auth import AwsCredentialsProvider
from enum import IntEnum


class AwsS3RequestType(IntEnum):
    """The type of the Aws S3 request"""

    DEFAULT = 0
    """
    Default type, which is all the rest type of S3 requests besides of GET_OBJECT/PUT_OBJECT
    """

    GET_OBJECT = 1
    """
    Get Object S3 request
    """

    PUT_OBJECT = 2
    """
    Put Object S3 request
    """


class S3Client(NativeResource):
    """S3 client

    Args:
        bootstrap (ClientBootstrap): Client bootstrap to use when initiating socket connection.

        region (str): Region that the S3 bucket lives in.

        credential_provider (Optional[AwsCredentialsProvider]): Credentials providers source the
            AwsCredentials needed to sign an authenticated AWS request.
            If None is provided, then the default chain credential provider will be used

        tls_connection_options (Optional[TlsConnectionOptions]): Optional TLS
            connection options. If None is provided, then the connection will
            be attempted over plain-text.

        part_size (Optional[int]): Size of parts the files will be downloaded or uploaded in.

        connection_timeout_ms (Optional[int]): Timeout value, in milliseconds, used for each connection.

        throughput_target_gbps (Optional[float]): Throughput target in Gbps that we are trying to reach.

        throughput_per_vip_gbps (Optional[float]): Amount of throughput in Gbps to designate to each VIP.

        num_connections_per_vip (Optional[int]): The number of connections that each VIP will have.
    """

    __slots__ = ('shutdown_future')

    def __init__(
            self,
            *,
            bootstrap,
            region,
            credential_provider=None,
            tls_connection_options=None,
            part_size=0,
            connection_timeout_ms=0,
            throughput_target_gbps=0,
            throughput_per_vip_gbps=0,
            num_connections_per_vip=0):
        assert isinstance(bootstrap, ClientBootstrap)
        assert isinstance(region, str)
        assert isinstance(credential_provider, AwsCredentialsProvider) or credential_provider is None
        assert isinstance(tls_connection_options, TlsConnectionOptions) or tls_connection_options is None
        assert isinstance(part_size, int) or part_size is None
        assert isinstance(connection_timeout_ms, int) or connection_timeout_ms is None
        assert isinstance(
            throughput_target_gbps,
            int) or isinstance(
            throughput_target_gbps,
            float) or throughput_target_gbps is None
        assert isinstance(
            throughput_per_vip_gbps,
            int) or isinstance(
            throughput_per_vip_gbps,
            float) or throughput_per_vip_gbps is None
        assert isinstance(num_connections_per_vip, int) or num_connections_per_vip is None

        if not credential_provider:
            credential_provider = AwsCredentialsProvider.new_default_chain(bootstrap)

        super().__init__()

        shutdown_future = Future()

        def on_shutdown():
            shutdown_future.set_result(None)

        self.shutdown_future = shutdown_future

        self._binding = _awscrt.s3_client_new(
            bootstrap,
            credential_provider,
            tls_connection_options,
            on_shutdown,
            region,
            part_size,
            connection_timeout_ms,
            throughput_target_gbps,
            throughput_per_vip_gbps,
            num_connections_per_vip)

    def make_request(self, *, request, type, on_headers=None, on_body=None):
        """Create the Request to the the S3 server,
        accelerate the GET_OBJECT/PUT_OBJECT request by spliting it into multiple requests under the hood.

        Args:
            request (HttpRequest): The overall outgoing API request for S3 operation.

            type (AwsS3RequestType): The type of S3 request passed in, GET_OBJECT/PUT_OBJECT can be accelerated

            on_headers: Optional callback invoked as the response received, and even the API request
                has been splitted into multiple parts, this callback will only be invoked once as
                it's just making one API request to S3.
                The function should take the following arguments and return nothing:

                *   `status_code` (int): Response status code.

                *   `headers` (List[Tuple[str, str]]): Response headers as a
                    list of (name,value) pairs.

                *   `**kwargs` (dict): Forward compatibility kwargs.

            on_body: Optional callback invoked 0+ times as the response body received from S3 server.
                The function should take the following arguments and return nothing:

                *   `chunk` (buffer): Response body data (not necessarily
                    a whole "chunk" of chunked encoding).

                *   `**kwargs` (dict): Forward-compatibility kwargs.

        Returns:
            S3Request
        """
        return S3Request(client=self, request=request, type=type, on_headers=on_headers, on_body=on_body)


class S3Request(NativeResource):
    """S3 request

    """
    __slots__ = ('_on_headers_cb', '_on_body_cb', '_finished_future', '_shutdown_future', "_client")

    def __init__(self, *, client, request, type, on_headers=None, on_body=None):
        assert isinstance(client, S3Client)
        assert isinstance(request, HttpRequest)
        assert callable(on_headers) or on_headers is None
        assert callable(on_body) or on_body is None

        super().__init__()

        # the native s3-request will keep the request alive until the s3-request finishes
        self._on_headers_cb = on_headers
        self._on_body_cb = on_body
        # to keep the client alive until the request shutdown
        self._client = client

        self._finished_future = Future()
        self._shutdown_future = Future()

        self._binding = _awscrt.s3_client_make_meta_request(
            client, request, type, self._on_headers, self._on_body, self._on_finish, self._on_shutdown)

    def _on_headers(self, status_code, headers):
        if self._on_headers_cb:
            self._on_headers_cb(status_code=status_code, headers=headers)

    def _on_body(self, chunk):
        if self._on_body_cb:
            self._on_body_cb(chunk=chunk)

    def _on_finish(self, error_code):
        if error_code:
            self._finished_future.set_exception(_awscrt.exceptions.from_code(error_code))
        else:
            self._finished_future.set_result("what?")

    def _on_shutdown(self):
        self.shutdown_future.set_result("what?")

    @property
    def finished_future(self):
        return self._finished_future

    @property
    def shutdown_future(self):
        return self._shutdown_future
