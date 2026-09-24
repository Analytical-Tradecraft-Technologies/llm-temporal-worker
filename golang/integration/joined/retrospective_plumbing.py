#!/usr/bin/env python3
"""Local storage/ledger boundary; never a research or inference provider.

The existing joined service supplies only ATT, fake workload credentials, S3,
and Temporal transport. Its synthetic model/search/platform handlers are not
reachable from either worker network. Successful artifact writes are retained
on the host before their acknowledgements reach callers.
"""

import hashlib
import http.client
import http.server
import json
import os
from pathlib import Path
import select
import socket
import socketserver
import ssl
import threading
from urllib.parse import urlsplit


ROOT = Path("/retained/plumbing")
LOCK = threading.Lock()
MAX_BODY = 32 << 20
CONTEXT = ssl.create_default_context(cafile="/run/smoke/ca.pem")


def append_record(name, value):
    with LOCK:
        with (ROOT / name).open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n")
            stream.flush()
            os.fsync(stream.fileno())


def archive(body):
    digest = hashlib.sha256(body).hexdigest()
    path = ROOT / "objects" / digest
    with LOCK:
        if not path.exists():
            with path.open("xb") as stream:
                stream.write(body)
                stream.flush()
                os.fsync(stream.fileno())
    return digest


class Boundary(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        # Do not log signed request headers, query strings, or credentials.
        pass

    def do_GET(self):
        self.forward()

    def do_HEAD(self):
        self.forward()

    def do_POST(self):
        self.forward()

    def do_PUT(self):
        self.forward()

    def forward(self):
        path = urlsplit(self.path).path
        allowed = (
            self.command == "GET" and path in {"/readiness", "/readyz", "/credentials", "/evidence"}
            or self.command in {"GET", "POST"} and path.startswith("/internal/v1/competition/")
            or self.command in {"GET", "HEAD", "PUT"} and (
                path == "/artifacts-smoke-bucket" or path.startswith("/artifacts-smoke-bucket/")
            )
        )
        if not allowed or "%" in path or ".." in path or self.headers.get("Transfer-Encoding"):
            append_record("blocked.jsonl", {"method": self.command, "path_sha256": hashlib.sha256(path.encode()).hexdigest()})
            self.send_error(403, "evaluation permits local storage and ledger only")
            return
        try:
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 <= size <= MAX_BODY:
                raise ValueError("request body exceeds limit")
            body = self.rfile.read(size)
            if len(body) != size:
                raise ValueError("incomplete request")
            headers = {key: value for key, value in self.headers.items() if key.lower() not in {"connection", "transfer-encoding"}}
            connection = http.client.HTTPSConnection("joined-backend", 443, timeout=30, context=CONTEXT)
            try:
                connection.request(self.command, self.path, body=body, headers=headers)
                response = connection.getresponse()
                result = response.read(MAX_BODY + 1)
                if len(result) > MAX_BODY:
                    raise ValueError("response body exceeds limit")
                if self.command == "PUT" and 200 <= response.status < 300:
                    digest = archive(body)
                    if digest != self.headers.get("X-Amz-Meta-Sha256"):
                        raise ValueError("accepted artifact digest mismatch")
                    append_record("artifacts.jsonl", {"path": path, "sha256": digest, "bytes": len(body), "media_type": self.headers.get("X-Amz-Meta-Media-Type", ""), "status": response.status})
                elif path.startswith("/internal/v1/competition/"):
                    append_record("ledger.jsonl", {"path": path, "method": self.command, "status": response.status, "request_sha256": archive(body), "response_sha256": archive(result)})
                elif path == "/evidence" and response.status == 200:
                    (ROOT / "evidence.json").write_bytes(result)
                self.send_response(response.status)
                for key, value in response.getheaders():
                    if key.lower() not in {"connection", "transfer-encoding", "content-length", "server", "date"}:
                        self.send_header(key, value)
                self.send_header("Content-Length", response.getheader("Content-Length", str(len(result))) if self.command == "HEAD" else str(len(result)))
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(result)
            finally:
                connection.close()
        except Exception as error:
            append_record("errors.jsonl", {"error_type": type(error).__name__})
            self.close_connection = True
            self.send_error(502, "local evaluation boundary failed")


class TemporalTunnel(socketserver.BaseRequestHandler):
    def handle(self):
        with socket.create_connection(("joined-backend", 7243), timeout=10) as upstream:
            self.request.settimeout(30)
            upstream.settimeout(30)
            while True:
                ready, _, _ = select.select([self.request, upstream], [], [], 30)
                for source in ready:
                    chunk = source.recv(65536)
                    if not chunk:
                        return
                    target = upstream if source is self.request else self.request
                    target.sendall(chunk)


class TunnelServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def main():
    (ROOT / "objects").mkdir(parents=True, exist_ok=True)
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.minimum_version = ssl.TLSVersion.TLSv1_3
    tls.load_cert_chain("/run/smoke/server.pem", "/run/smoke/server-key.pem")
    server = http.server.ThreadingHTTPServer(("0.0.0.0", 443), Boundary)
    server.socket = tls.wrap_socket(server.socket, server_side=True)
    tunnel = TunnelServer(("0.0.0.0", 7243), TemporalTunnel)
    threading.Thread(target=tunnel.serve_forever, daemon=True).start()
    server.serve_forever()


if __name__ == "__main__":
    main()
