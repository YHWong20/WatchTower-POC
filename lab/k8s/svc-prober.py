"""svc-prober: call the kubernetes Service VIP from the node's own network every 5 s.

Runs with hostNetwork, so its calls take the same path as the kubelet's and the control
plane's own calls to https://10.96.0.1:443 (172.30.0.1 on OpenShift): kube-proxy picks one API
server per connection, and calls to any other node cross the switch. One JSON line per call:
    {"node": "...", "target": "...", "ok": true, "ms": 12, "err": null}
Errors keep the Go client's wording ("dial tcp 10.96.0.1:443: i/o timeout") because that is
what the real OpenShift components log and what the Splunk searches look for.
Standard library only; a fresh connection per call so every call is load-balanced again.
"""

import json
import os
import socket
import ssl
import sys
import time
from urllib.parse import urlsplit

NODE = os.environ.get("NODE_NAME", socket.gethostname())
TARGET = os.environ.get("TARGET", "https://10.96.0.1:443/readyz")
INTERVAL_S = float(os.environ.get("INTERVAL_S", "5"))
TIMEOUT_S = float(os.environ.get("TIMEOUT_S", "2"))
SA = "/var/run/secrets/kubernetes.io/serviceaccount"

url = urlsplit(TARGET)
HOST, PORT, PATH = url.hostname, url.port or 443, url.path or "/"
ctx = ssl.create_default_context(cafile=f"{SA}/ca.crt")


def probe() -> tuple[bool, str | None]:
    deadline = time.monotonic() + TIMEOUT_S
    try:
        sock = socket.create_connection((HOST, PORT), timeout=TIMEOUT_S)
    except (socket.timeout, TimeoutError):
        return False, f"dial tcp {HOST}:{PORT}: i/o timeout"
    except OSError as exc:
        return False, f"dial tcp {HOST}:{PORT}: connect: {(exc.strerror or str(exc)).lower()}"
    local = "%s:%d" % sock.getsockname()[:2]
    try:
        sock.settimeout(max(deadline - time.monotonic(), 0.01))
        with ctx.wrap_socket(sock, server_hostname=HOST) as tls:
            with open(f"{SA}/token") as f:
                token = f.read().strip()
            tls.sendall(f"GET {PATH} HTTP/1.1\r\nHost: {HOST}\r\nAuthorization: Bearer {token}\r\n"
                        "User-Agent: svc-prober/1.0\r\nConnection: close\r\n\r\n".encode())
            status_line = tls.recv(64).split(b"\r\n", 1)[0].decode(errors="replace")
    except (socket.timeout, TimeoutError):
        return False, f"read tcp {local}->{HOST}:{PORT}: i/o timeout"
    except (ssl.SSLError, OSError) as exc:
        return False, f"read tcp {local}->{HOST}:{PORT}: {exc}"
    finally:
        sock.close()
    parts = status_line.split()
    if len(parts) < 2 or parts[1] != "200":
        return False, f"unexpected status: {status_line or 'empty reply'}"
    return True, None


def main() -> None:
    next_at = time.monotonic()
    while True:
        start = time.monotonic()
        ok, err = probe()
        ms = int((time.monotonic() - start) * 1000)
        sys.stdout.write(json.dumps({"node": NODE, "target": TARGET, "ok": ok, "ms": ms, "err": err}) + "\n")
        sys.stdout.flush()
        next_at += INTERVAL_S
        time.sleep(max(next_at - time.monotonic(), 0))


if __name__ == "__main__":
    main()
