import io
import socket
import unittest
from unittest import mock

import historical_archive_tunnel


class RequestSocket:
    def __init__(self, request):
        self.input = io.BytesIO(request)
        self.output = bytearray()

    def settimeout(self, _timeout):
        pass

    def makefile(self, _mode, _buffering):
        return self.input

    def sendall(self, value):
        self.output.extend(value)


class ArchiveTunnelTest(unittest.TestCase):
    def request(self, wire, addresses=()):
        connection = RequestSocket(wire)
        with mock.patch.object(socket, "getaddrinfo", return_value=addresses), mock.patch.object(socket, "socket", side_effect=AssertionError("rejected request attempted external dial")):
            historical_archive_tunnel.ArchiveTunnel(connection, ("127.0.0.1", 1), None)
        return bytes(connection.output)

    def test_rejects_general_forward_proxy_and_wrong_connect_authority(self):
        for request in (
            b"GET https://web.archive.org/web/20250101000000id_/https://example.com HTTP/1.1\r\n\r\n",
            b"CONNECT example.com:443 HTTP/1.1\r\n\r\n",
            b"CONNECT web.archive.org:80 HTTP/1.1\r\n\r\n",
            b"CONNECT user:password@web.archive.org:443 HTTP/1.1\r\n\r\n",
        ):
            with self.subTest(request=request):
                self.assertTrue(self.request(request).startswith(b"HTTP/1.1 403 "))

    def test_rejects_mixed_public_and_private_dns_answers(self):
        addresses = [
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("8.8.8.8", 443)),
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("127.0.0.1", 443)),
        ]
        response = self.request(b"CONNECT web.archive.org:443 HTTP/1.1\r\nHost: web.archive.org:443\r\n\r\n", addresses)
        self.assertTrue(response.startswith(b"HTTP/1.1 502 "))

    def test_rejects_ipv6_private_archive_address(self):
        addresses = [(socket.AF_INET6, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("::1", 443, 0, 0))]
        response = self.request(b"CONNECT web.archive.org:443 HTTP/1.1\r\n\r\n", addresses)
        self.assertTrue(response.startswith(b"HTTP/1.1 502 "))


if __name__ == "__main__":
    unittest.main()
