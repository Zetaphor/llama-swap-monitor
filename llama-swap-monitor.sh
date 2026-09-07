#!/usr/bin/env bash
# Outputs llama-swap status as a single line for the KDE Command Output widget.
# Configure the widget to run this script every 2–4 seconds.

CONFIG_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/llama-swap-monitor"
CONFIG_FILE="$CONFIG_DIR/config"
if [[ -f "$CONFIG_FILE" ]]; then
  # shellcheck source=/dev/null
  source "$CONFIG_FILE"
fi

export LLAMA_SWAP_API="${LLAMA_SWAP_API:-http://127.0.0.1:10080}"
export LLAMA_SWAP_MONITOR_STATE_FILE="${LLAMA_SWAP_MONITOR_STATE_FILE:-${XDG_RUNTIME_DIR:-/tmp}/llama-swap-monitor-state.json}"

/usr/bin/python3 <<'PY'
import json
import os
import re
import socket
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE = os.environ.get("LLAMA_SWAP_API", "http://127.0.0.1:10080").rstrip("/")
HOST = urllib.parse.urlparse(BASE).hostname or "127.0.0.1"
API_KEY = os.environ.get("LLAMA_SWAP_API_KEY", "").strip()
STATE_FILE = os.environ.get("LLAMA_SWAP_MONITOR_STATE_FILE", "/tmp/llama-swap-monitor-state.json")

TIMEOUT_FAST = 0.35
TIMEOUT_NORMAL = 0.45
TIMEOUT_SSE_CONNECT = 0.6
SSE_WINDOW_SECONDS = 0.35
SSE_MAX_EVENTS = 20
PROMPT_PCT_STICKY_SECONDS = 5.0
GEN_TPS_HOLD_SECONDS = 1.5
GEN_TPS_EMA_ALPHA = 0.55
OFFLINE_GRACE_SECONDS = 15.0


def request_for(path: str, accept: str = "application/json"):
    headers = {"Accept": accept}
    if API_KEY:
        headers["Authorization"] = f"Bearer {API_KEY}"
    return urllib.request.Request(BASE + path, headers=headers)


def fetch_json(path: str, timeout: float):
    req = request_for(path, "application/json")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
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
    for line in log_text.splitlines():
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
        prompt_total = slot.get("n_prompt_tokens") or 0
        prompt_done = slot.get("n_prompt_tokens_processed") or 0
        return {
            "slot": slot.get("id"),
            "n_decoded": nt.get("n_decoded") or 0,
            "n_prompt_tokens": int(prompt_total) if isinstance(prompt_total, int) else 0,
            "n_prompt_tokens_processed": int(prompt_done) if isinstance(prompt_done, int) else 0,
        }
    return None


def parse_outer_event(raw_payload: str):
    try:
        outer = json.loads(raw_payload)
    except Exception:
        return None, None
    kind = outer.get("type")
    inner = outer.get("data")
    if isinstance(inner, str):
        try:
            inner = json.loads(inner)
        except Exception:
            pass
    return kind, inner


def read_sse_snapshot():
    models = []
    upstream_chunks = []
    inflight = None
    sse_ok = False
    got_model_status = False
    got_upstream_log = False
    event_count = 0
    started = time.monotonic()
    deadline = started + SSE_WINDOW_SECONDS
    req = request_for("/api/events", "text/event-stream")

    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_SSE_CONNECT) as resp:
            sse_ok = True
            data_lines = []
            while time.monotonic() < deadline and event_count < SSE_MAX_EVENTS:
                try:
                    raw = resp.readline()
                except socket.timeout:
                    break
                if not raw:
                    break

                line = raw.decode("utf-8", "replace").rstrip("\r\n")
                if line == "":
                    if not data_lines:
                        continue
                    payload = "\n".join(data_lines)
                    data_lines = []
                    kind, inner = parse_outer_event(payload)
                    if not kind:
                        continue
                    event_count += 1
                    if kind == "modelStatus" and isinstance(inner, list):
                        models = inner
                        got_model_status = True
                    elif kind == "logData" and isinstance(inner, dict):
                        if inner.get("source") == "upstream":
                            text = inner.get("data")
                            if isinstance(text, str) and text:
                                upstream_chunks.append(text)
                                got_upstream_log = True
                    elif kind == "inflight":
                        inflight = inner

                    if got_model_status and got_upstream_log and event_count >= 3:
                        break
                    continue

                if line.startswith("data:"):
                    data_lines.append(line[5:].lstrip())
    except Exception:
        pass

    return {
        "ok": sse_ok,
        "models": models,
        "upstream_log": "\n".join(upstream_chunks),
        "inflight": inflight,
    }


def inflight_activity(inflight_obj):
    if not isinstance(inflight_obj, dict):
        return False, None
    op = inflight_obj.get("operation")
    if op == "remove":
        return False, None
    if op == "snapshot":
        reqs = inflight_obj.get("requests") or []
        if reqs:
            model = reqs[0].get("model")
            return True, model
        return False, None
    req = inflight_obj.get("request")
    if isinstance(req, dict):
        return True, req.get("model")
    return False, None


def alive():
    try:
        fetch_json("/api/version", TIMEOUT_FAST)
        return True
    except Exception:
        pass
    try:
        req = request_for("/health", "text/plain")
        with urllib.request.urlopen(req, timeout=TIMEOUT_FAST):
            return True
    except Exception:
        return False


def build_state_map(models, running):
    state_map = {}
    for model in models:
        mid = model.get("id")
        state = model.get("state")
        if mid and state:
            state_map[mid] = state
    if state_map:
        return state_map
    for proc in running:
        mid = proc.get("model") or proc.get("name")
        state = proc.get("state")
        if mid and state:
            state_map[mid] = state
    return state_map


def load_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {}


def save_state(data):
    try:
        parent = os.path.dirname(STATE_FILE)
        if parent:
            os.makedirs(parent, exist_ok=True)
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, separators=(",", ":"))
        os.replace(tmp, STATE_FILE)
    except Exception:
        pass


def compute_gen_tps(state_cache, model, decoded, now_ts):
    if not model or decoded <= 0:
        return None

    prev_model = state_cache.get("gen_prev_model")
    prev_decoded = state_cache.get("gen_prev_decoded")
    prev_ts = state_cache.get("gen_prev_ts")
    prev_ema = state_cache.get("gen_tps_ema")
    prev_ema_ts = state_cache.get("gen_tps_ema_ts")
    raw_tps = None

    if (
        prev_model == model
        and isinstance(prev_decoded, int)
        and isinstance(prev_ts, (int, float))
    ):
        dt = now_ts - float(prev_ts)
        dd = decoded - prev_decoded
        if dt >= 0.05 and dd >= 0:
            raw_tps = dd / dt

    if raw_tps is not None:
        if isinstance(prev_ema, (int, float)):
            tps = (GEN_TPS_EMA_ALPHA * raw_tps) + ((1.0 - GEN_TPS_EMA_ALPHA) * float(prev_ema))
        else:
            tps = raw_tps
        state_cache["gen_tps_ema"] = tps
        state_cache["gen_tps_ema_ts"] = now_ts
    elif isinstance(prev_ema, (int, float)) and isinstance(prev_ema_ts, (int, float)):
        if now_ts - float(prev_ema_ts) <= GEN_TPS_HOLD_SECONDS:
            tps = float(prev_ema)
        else:
            tps = None
    else:
        tps = None

    state_cache["gen_prev_model"] = model
    state_cache["gen_prev_decoded"] = int(decoded)
    state_cache["gen_prev_ts"] = now_ts
    return tps


state_cache = load_state()
now_ts = time.time()

running_ok = False
try:
    running_resp = fetch_json("/running", TIMEOUT_NORMAL)
    running = running_resp.get("running", []) if isinstance(running_resp, dict) else []
    running_ok = True
except Exception:
    running = []

total = None
models_ok = False
try:
    models_resp = fetch_json("/v1/models", TIMEOUT_NORMAL)
    total = len(models_resp.get("data", []))
    models_ok = True
except Exception:
    try:
        models_resp = fetch_json("/models", TIMEOUT_NORMAL)
        total = len(models_resp.get("data", []))
        models_ok = True
    except Exception:
        total = None

sse = read_sse_snapshot()
sse_ok = bool(sse.get("ok"))
connectivity_ok = bool(running_ok or models_ok or sse_ok)
if not connectivity_ok:
    last_ok_ts = state_cache.get("last_ok_ts")
    last_line = state_cache.get("last_line")
    if (
        isinstance(last_ok_ts, (int, float))
        and isinstance(last_line, str)
        and last_line
        and (now_ts - float(last_ok_ts) <= OFFLINE_GRACE_SECONDS)
    ):
        print(last_line)
        sys.exit(0)
    offline()

models = sse["models"] if isinstance(sse.get("models"), list) else []
upstream_log = sse["upstream_log"] if isinstance(sse.get("upstream_log"), str) else ""
inflight = sse.get("inflight")

ports = {}
for proc in running:
    proxy = proc.get("proxy") or ""
    port = urllib.parse.urlparse(proxy).port
    if port:
        ports[port] = proc.get("model") or proc.get("name") or "?"

slot_activity = None
slot_model = None
log_activity = latest_upstream_activity(upstream_log)
if ports:
    with ThreadPoolExecutor(max_workers=len(ports)) as pool:
        futures = [pool.submit(fetch_slots, port, 0.25) for port in ports]
        for fut in as_completed(futures, timeout=0.45):
            try:
                port, slots = fut.result()
            except Exception:
                continue
            active = active_slot(slots)
            if active:
                slot_activity = active
                slot_model = ports.get(port)
                break

inflight_active, inflight_model = inflight_activity(inflight)

state_map = build_state_map(models, running)
starting = sorted([mid for mid, st in state_map.items() if st == "starting"])
stopping = sorted([mid for mid, st in state_map.items() if st == "stopping"])
loaded = len(running)
if not running_ok:
    loaded_from_models = sum(1 for st in state_map.values() if st in ("ready", "starting", "stopping"))
    if loaded_from_models > 0:
        loaded = loaded_from_models
    elif isinstance(state_cache.get("last_loaded"), int):
        loaded = int(state_cache["last_loaded"])
if total is None and isinstance(state_cache.get("last_total"), int):
    total = int(state_cache["last_total"])
model = slot_model

if not model and inflight_model:
    model = inflight_model

if not model and len(running) == 1:
    model = running[0].get("model") or running[0].get("name")

has_live_activity = bool(slot_activity or inflight_active)

if has_live_activity:
    decoded = slot_activity["n_decoded"] if slot_activity else 0
    prompt_pct = None
    prompt_pct_source = None
    if decoded > 0:
        prompt_pct = None
    elif slot_activity:
        p_total = slot_activity.get("n_prompt_tokens", 0)
        p_done = slot_activity.get("n_prompt_tokens_processed", 0)
        if p_total > 0:
            prompt_pct = max(0, min(100, int((p_done * 100) / p_total)))
            prompt_pct_source = "slots"
    elif log_activity and log_activity[0] == "prompt":
        prompt_pct = max(0, min(100, int(log_activity[1] * 100)))
        prompt_pct_source = "log"
    elif decoded == 0:
        cached_pct = state_cache.get("prompt_pct")
        cached_ts = state_cache.get("prompt_ts")
        if isinstance(cached_pct, int) and isinstance(cached_ts, (int, float)):
            if now_ts - float(cached_ts) <= PROMPT_PCT_STICKY_SECONDS:
                prompt_pct = max(0, min(100, cached_pct))
                prompt_pct_source = "cache"

    if decoded > 0:
        tps = compute_gen_tps(state_cache, model, decoded, now_ts)
        if isinstance(tps, (int, float)) and tps > 0.05:
            label = f"⚡ Gen {decoded}t {tps:.1f} tok/s"
        else:
            label = f"⚡ Gen {decoded}t"
    elif prompt_pct is not None:
        label = f"🟡 Prompt {prompt_pct}%"
    elif inflight_active:
        label = "🟡 Busy"
    else:
        label = "🟡 Prompt"

    if prompt_pct is not None and prompt_pct_source in ("slots", "log"):
        state_cache["prompt_pct"] = int(prompt_pct)
        state_cache["prompt_ts"] = now_ts
    elif decoded > 0:
        state_cache.pop("prompt_pct", None)
        state_cache.pop("prompt_ts", None)
    else:
        state_cache.pop("gen_prev_model", None)
        state_cache.pop("gen_prev_decoded", None)
        state_cache.pop("gen_prev_ts", None)
        state_cache.pop("gen_tps_ema", None)
        state_cache.pop("gen_tps_ema_ts", None)
    if model:
        out_line = f"{label} · {model}"
        print(out_line)
        state_cache["last_model"] = model
    else:
        out_line = label
        print(out_line)
    state_cache["last_phase"] = "active"
    state_cache["last_line"] = out_line
    state_cache["last_ok_ts"] = now_ts
    state_cache["last_loaded"] = loaded
    if total is not None:
        state_cache["last_total"] = total
    save_state(state_cache)
elif starting:
    out_line = f"🟡 Starting · {starting[0]}"
    print(out_line)
    state_cache.pop("prompt_pct", None)
    state_cache.pop("prompt_ts", None)
    state_cache.pop("gen_prev_model", None)
    state_cache.pop("gen_prev_decoded", None)
    state_cache.pop("gen_prev_ts", None)
    state_cache.pop("gen_tps_ema", None)
    state_cache.pop("gen_tps_ema_ts", None)
    state_cache["last_phase"] = "starting"
    state_cache["last_line"] = out_line
    state_cache["last_ok_ts"] = now_ts
    state_cache["last_loaded"] = loaded
    if total is not None:
        state_cache["last_total"] = total
    save_state(state_cache)
elif stopping:
    out_line = f"🟡 Stopping · {stopping[0]}"
    print(out_line)
    state_cache.pop("prompt_pct", None)
    state_cache.pop("prompt_ts", None)
    state_cache.pop("gen_prev_model", None)
    state_cache.pop("gen_prev_decoded", None)
    state_cache.pop("gen_prev_ts", None)
    state_cache.pop("gen_tps_ema", None)
    state_cache.pop("gen_tps_ema_ts", None)
    state_cache["last_phase"] = "stopping"
    state_cache["last_line"] = out_line
    state_cache["last_ok_ts"] = now_ts
    state_cache["last_loaded"] = loaded
    if total is not None:
        state_cache["last_total"] = total
    save_state(state_cache)
else:
    if total is None:
        out_line = f"🟢 Idle · {loaded} loaded"
        print(out_line)
    else:
        out_line = f"🟢 Idle · {loaded} loaded / {total}"
        print(out_line)
    state_cache.pop("prompt_pct", None)
    state_cache.pop("prompt_ts", None)
    state_cache.pop("gen_prev_model", None)
    state_cache.pop("gen_prev_decoded", None)
    state_cache.pop("gen_prev_ts", None)
    state_cache.pop("gen_tps_ema", None)
    state_cache.pop("gen_tps_ema_ts", None)
    state_cache["last_phase"] = "idle"
    state_cache["last_line"] = out_line
    state_cache["last_ok_ts"] = now_ts
    state_cache["last_loaded"] = loaded
    if total is not None:
        state_cache["last_total"] = total
    save_state(state_cache)
PY
