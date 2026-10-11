"""Offline readiness: does the running lab need the internet? (make offline-check)

    uv run python lab/offline_check.py [--duration 300] [-- make demo MODE=investigate]

1. Static: every image the lab uses is local (no pull needed), the model is in Ollama, and the
   Copilot's pages load nothing from outside (HTMX and Chart.js are vendored).
2. Live: while the given command runs (or for --duration seconds), samples the Docker VM's
   connection-tracking table every 3 s (conntrack, from the kindest/node image already on the
   Mac) and lists every flow to a public address. Expected: none. That is what a run with Wi-Fi
   off depends on; it does not replace one, since macOS itself is not watched.
"""

from __future__ import annotations

import argparse
import ipaddress
import json
import re
import subprocess
import sys
import threading
import time
import urllib.request

LOCAL_NETS = [ipaddress.ip_network(n) for n in (
    "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "127.0.0.0/8", "169.254.0.0/16", "0.0.0.0/8",
    "100.64.0.0/10", "198.18.0.0/15", "224.0.0.0/4", "255.255.255.255/32",
    "::1/128", "fc00::/7", "fe80::/10", "ff00::/8")]
IMAGES = ["python:3.12-slim", "redis:7-alpine", "watchtower:dev", "registry.k8s.io/metrics-server/metrics-server:v0.7.2"]


def public(ip: str) -> bool:
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return False
    return not any(addr in n for n in LOCAL_NETS if n.version == addr.version)


def ok(good: bool, text: str) -> bool:
    print(("   ok   " if good else "   FAIL ") + text, flush=True)
    return good


def static_checks() -> bool:
    good = True
    kind_img = subprocess.run(["docker", "inspect", "ocp-sim-worker", "--format", "{{.Config.Image}}"],
                              capture_output=True, text=True).stdout.strip()
    for img in IMAGES + ([kind_img] if kind_img else []):
        present = subprocess.run(["docker", "image", "inspect", img], capture_output=True).returncode == 0
        good &= ok(present, f"image {img.split('@')[0]} is local")
    pods = json.loads(subprocess.run(["kubectl", "--context", "kind-ocp-sim", "get", "pods", "-A", "-o", "json"],
                                     capture_output=True, text=True).stdout or '{"items": []}')
    always = [c["image"] for p in pods["items"] for c in p["spec"]["containers"] if c.get("imagePullPolicy") == "Always"]
    good &= ok(not always, "no pod pulls its image on every start (imagePullPolicy Always)" +
               (f": {always}" if always else ""))
    try:
        tags = json.load(urllib.request.urlopen("http://127.0.0.1:11434/api/tags", timeout=5))
        names = sorted(m["name"] for m in tags["models"])
        ok(True, f"Ollama models on the Mac: {', '.join(names)}")
    except OSError:
        ok(True, "Ollama not running (WT_LLM=off works without it)")
    try:
        html = "".join(urllib.request.urlopen(f"http://127.0.0.1:8080{p}", timeout=10).read().decode()
                       for p in ("/", "/investigations"))
        external = sorted(set(re.findall(r'(?:src|href)="(https?://[^"]+)"', html)))
        good &= ok(not external, "the Copilot loads nothing from outside" + (f": {external}" if external else ""))
    except OSError:
        ok(True, "Copilot not running: page check skipped")
    return good


def watch(stop: threading.Event, seen: dict[tuple[str, str], int], image: str) -> None:
    while not stop.is_set():
        out = subprocess.run(["docker", "run", "--rm", "--net", "host", "--privileged", "--entrypoint", "conntrack",
                              image, "-L"], capture_output=True, text=True).stdout
        for line in out.splitlines():
            m = re.search(r"src=(\S+) dst=(\S+)(?: sport=\d+ dport=(\d+))?", line)
            if m and public(m.group(2)):
                key = (m.group(2), m.group(3) or "-")
                seen[key] = seen.get(key, 0) + 1
        stop.wait(3)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=float, default=0)
    ap.add_argument("command", nargs=argparse.REMAINDER)
    args = ap.parse_args()
    cmd = args.command[1:] if args.command[:1] == ["--"] else args.command
    print("offline readiness: static checks", flush=True)
    good = static_checks()
    image = subprocess.run(["docker", "inspect", "ocp-sim-worker", "--format", "{{.Config.Image}}"],
                           capture_output=True, text=True).stdout.strip()
    if not image or not (cmd or args.duration):
        print("\nPASS" if good else "\nFAIL")
        return 0 if good else 1
    print(f"\nwatching the Docker VM's connections while {'`' + ' '.join(cmd) + '` runs' if cmd else f'{args.duration:.0f} s pass'}",
          flush=True)
    seen: dict[tuple[str, str], int] = {}
    stop = threading.Event()
    t = threading.Thread(target=watch, args=(stop, seen, image), daemon=True)
    t.start()
    rc = subprocess.run(cmd).returncode if cmd else (time.sleep(args.duration) or 0)
    stop.set()
    t.join()
    good &= ok(not seen, "no connection from the lab to a public address" +
               ("" if not seen else ": " + ", ".join(f"{ip}:{port} ({n} samples)" for (ip, port), n in sorted(seen.items()))))
    if cmd:
        good &= ok(rc == 0, f"`{' '.join(cmd)}` exited {rc}")
    print("\nPASS" if good else "\nFAIL")
    return 0 if good else 1


if __name__ == "__main__":
    sys.exit(main())
