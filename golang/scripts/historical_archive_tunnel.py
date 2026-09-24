#!/usr/bin/env python3
"""Task-local CONNECT transport, not a search backend or TLS terminator."""

import ipaddress
import select
import socket
import socketserver
import threading
import time


SLOTS = threading.BoundedSemaphore(64)


class ArchiveTunnel(socketserver.StreamRequestHandler):
    def handle(self):
        if not SLOTS.acquire(blocking=False):
            self.request.sendall(b"HTTP/1.1 503 Busy\r\nConnection: close\r\n\r\n")
            return
        try:
            self.request.settimeout(10)
            request = self.rfile.readline(4097)
            if not request:  # Local TCP health check.
                return
            if request not in (b"CONNECT web.archive.org:443 HTTP/1.1\r\n", b"CONNECT web.archive.org:443 HTTP/1.0\r\n"):
                self.request.sendall(b"HTTP/1.1 403 Archive authority only\r\nConnection: close\r\n\r\n")
                return
            total = len(request)
            while True:
                line = self.rfile.readline(4097)
                total += len(line)
                if not line or len(line) > 4096 or total > 16384:
                    return
                if line == b"\r\n":
                    break
                if line.split(b":", 1)[0].lower() in {b"content-length", b"transfer-encoding"}:
                    return
            addresses = socket.getaddrinfo("web.archive.org", 443, type=socket.SOCK_STREAM)
            if not addresses or any(not ipaddress.ip_address(item[4][0]).is_global for item in addresses):
                self.request.sendall(b"HTTP/1.1 502 Nonpublic archive address\r\nConnection: close\r\n\r\n")
                return
            # No retries, redirects, alternate authority, or live-page fallback.
            family, kind, protocol, _, address = addresses[0]
            with socket.socket(family, kind, protocol) as upstream:
                upstream.settimeout(10)
                upstream.connect(address)
                self.request.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                self.request.settimeout(10)
                deadline = time.monotonic() + 120
                while time.monotonic() < deadline:
                    readable, _, _ = select.select([self.request, upstream], [], [], min(10, max(0, deadline - time.monotonic())))
                    for source in readable:
                        body = source.recv(65536)
                        if not body:
                            return
                        (upstream if source is self.request else self.request).sendall(body)
        except (OSError, ValueError):
            # No request headers, URLs, or credential material in logs.
            return
        finally:
            SLOTS.release()


class TunnelServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True
    request_queue_size = 64


if __name__ == "__main__":
    with TunnelServer(("0.0.0.0", 8080), ArchiveTunnel) as server:
        server.serve_forever()
