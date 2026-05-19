#!/usr/bin/env bash
# Outputs llama-swap status as a single line for the KDE Command Output widget.
# Configure the widget to run this script every 2–4 seconds.

export LLAMA_SWAP_API="${LLAMA_SWAP_API:-http://127.0.0.1:10080}"

/usr/bin/python3 <<'PY'
import json
import os
import re
import sys
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE = os.environ.get("LLAMA_SWAP_API", "http://127.0.0.1:10080").rstrip("/")
HOST = urllib.parse.urlparse(BASE).hostname or "127.0.0.1"


def fetch(path, timeout=3):
    with urllib.request.urlopen(BASE + path, timeout=timeout) as resp:
        return json.load(resp)


def offline():
    print("🔴 Offline")
    sys.exit(0)


def parse_ts(line: str):
    if " I " not in line:
        return ()
    head = line.split(" I ", 1)[0].strip().split()
    if not head:
        return ()
    try:
        return tuple(int(x) for x in head[-1].split("."))
    except ValueError:
        return ()


def latest_upstream_activity(log_text: str):
    best_ts = ()
    best = None
    for line in log_text.split("\n"):
        ts = parse_ts(line)
        if not ts:
            continue
        if "prompt processing" in line:
            m = re.search(r"progress = ([\d.]+)", line)
            if m and ts >= best_ts:
                best_ts, best = ts, ("prompt", float(m.group(1)))
        elif "n_decoded" in line and "tg =" in line:
            m = re.search(r"n_decoded =\s*(\d+)", line)
            if m and ts >= best_ts:
                best_ts, best = ts, ("gen", int(m.group(1)))
    return best


def fetch_slots(port: int, timeout=2):
    url = f"http://{HOST}:{port}/slots"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return port, json.load(resp)
    except Exception:
        return port, None


def active_slot(slots):
    if not slots:
        return None
    for slot in slots:
        if not slot.get("is_processing"):
            continue
        nt = (slot.get("next_token") or [{}])[0]
        return {
            "slot": slot.get("id"),
            "n_decoded": nt.get("n_decoded") or 0,
        }
    return None


try:
    fetch("/api/version")
except Exception:
    offline()

try:
    running = fetch("/running").get("running", [])
    total = len(fetch("/v1/models").get("data", []))
except Exception:
    offline()

upstream_log = ""
models = []
try:
    req = urllib.request.Request(f"{BASE}/api/events")
    with urllib.request.urlopen(req, timeout=4) as resp:
        for raw in resp:
            line = raw.decode().strip()
            if not line.startswith("data:"):
                continue
            msg = json.loads(line[5:])
            kind = msg.get("type")
            if kind == "logData":
                inner = json.loads(msg["data"])
                if inner.get("source") == "upstream":
                    upstream_log = inner["data"]
            elif kind == "modelStatus":
                models = json.loads(msg["data"])
            elif kind == "inflight":
                break
except Exception:
    pass

ports = {}
for proc in running:
    proxy = proc.get("proxy") or ""
    port = urllib.parse.urlparse(proxy).port
    if port:
        ports[port] = proc.get("model") or proc.get("name") or "?"

slot_activity = None
slot_model = None
if ports:
    with ThreadPoolExecutor(max_workers=len(ports)) as pool:
        futures = [pool.submit(fetch_slots, port, 2) for port in ports]
        for fut in as_completed(futures, timeout=3):
            port, slots = fut.result()
            active = active_slot(slots)
            if active:
                slot_activity = active
                slot_model = ports.get(port)
                break

log_activity = latest_upstream_activity(upstream_log)
slot_timeout = 5 if log_activity else 2

starting = [m["id"] for m in models if m.get("state") == "starting"]
stopping = [m["id"] for m in models if m.get("state") == "stopping"]
loaded = len(running)
model = slot_model

if log_activity and not model and ports:
    with ThreadPoolExecutor(max_workers=len(ports)) as pool:
        futures = [pool.submit(fetch_slots, port, slot_timeout) for port in ports]
        for fut in as_completed(futures, timeout=slot_timeout + 1):
            port, slots = fut.result()
            if active_slot(slots):
                model = ports.get(port)
                if not slot_activity:
                    slot_activity = active_slot(slots)
                break

if not model and len(running) == 1:
    model = running[0].get("model")

if slot_activity or log_activity:
    decoded = slot_activity["n_decoded"] if slot_activity else 0
    if log_activity and log_activity[0] == "prompt":
        pct = int(log_activity[1] * 100)
        label = f"🟡 Prompt {pct}%"
    elif decoded > 0:
        label = f"⚡ Gen {decoded}t"
    else:
        label = "🟡 Prompt"
    if model:
        print(f"{label} · {model}")
    else:
        print(label)
elif starting:
    print(f"🟡 Starting · {starting[0]}")
elif stopping:
    print(f"🟡 Stopping · {stopping[0]}")
else:
    print(f"🟢 Idle · {loaded} loaded / {total}")
PY
