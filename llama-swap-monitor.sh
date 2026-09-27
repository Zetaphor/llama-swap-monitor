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
import calendar
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
SSE_INFLIGHT_GRACE_SECONDS = 0.10
PROMPT_PCT_STICKY_SECONDS = 5.0
GEN_TPS_HOLD_SECONDS = 1.5
GEN_TPS_EMA_ALPHA = 0.55
GUFO_PHASE_HOLD_SECONDS = 8.0
# gufo --log-progress emits one line per prefill chunk and every 50 decoded
# tokens (~1-1.5 s apart in practice). A line older than this means generation
# is not actually advancing, so stop replaying it.
GUFO_PROGRESS_FRESH_SECONDS = 6.0
OFFLINE_GRACE_SECONDS = 15.0


def clear_stale_state():
    for k in ("prompt_pct", "prompt_ts", "gen_prev_model", "gen_prev_decoded", "gen_prev_ts", "gen_tps_ema", "gen_tps_ema_ts"):
        state_cache.pop(k, None)


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


GUFO_PROGRESS_RE = re.compile(
    r"\[progress\]\s+request=(\d+)\s+phase=(prefill|decode)\s+tokens=(\d+)/(\d+)\s+"
    r"percentage=([\d.]+)\s+chunk_tps=([\d.]+)\s+avg_tps=([\d.]+)"
)
GUFO_DRAFT_RE = re.compile(
    r"draft_accepted=(\d+)\s+draft_proposed=(\d+)\s+acceptance_percentage=([\d.]+)"
)


def gufo_progress_epoch(line: str):
    """gufo stamps its own log lines in UTC (`YYYY-MM-DD HH:MM:SS` before the
    [INFO] tag). Return the epoch, or None when the line is untimestamped."""
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})", line)
    if not m:
        return None
    try:
        return calendar.timegm(tuple(int(x) for x in m.groups()) + (0, 0, 0))
    except (OverflowError, ValueError):
        return None


def latest_gufo_progress(log_text: str):
    """Parse gufo's --log-progress output into scheduler-authoritative live
    state, or None when the backend is not gufo or the flag is off.

    Shapes (verified against engine b722a61):
      [progress] request=1 phase=prefill tokens=113664/134093 percentage=84.8 \
                 chunk_tps=791.9 avg_tps=842.1
      [progress] request=2 phase=decode tokens=351/600 percentage=58.5 \
                 chunk_tps=36.0 avg_tps=33.9 draft_accepted=144 \
                 draft_proposed=212 acceptance_percentage=67.9

    The newest line in the burst wins, which is whatever the scheduler most
    recently advanced -- with --sessions 2 a prefilling request legitimately
    interleaves with a decoding one. Lines older than GUFO_PROGRESS_FRESH_SECONDS
    are dropped so a finished request stops replaying.
    """
    best = None
    best_epoch = None
    now = time.time()
    for line in log_text.splitlines():
        if "[progress]" not in line:
            continue
        m = GUFO_PROGRESS_RE.search(line)
        if not m:
            continue
        epoch = gufo_progress_epoch(line)
        if epoch is not None and now - epoch > GUFO_PROGRESS_FRESH_SECONDS:
            continue
        req, phase, done, total, pct, chunk_tps, avg_tps = m.groups()
        entry = {
            "req": int(req),
            "kind": "gen" if phase == "decode" else "prompt",
            "done": int(done),
            "total": int(total),
            "pct": float(pct),
            "tps": float(avg_tps),
            "epoch": epoch,
        }
        d = GUFO_DRAFT_RE.search(line)
        if d:
            entry["draft_accepted"] = int(d.group(1))
            entry["draft_proposed"] = int(d.group(2))
            entry["acceptance_pct"] = float(d.group(3))
        # Keep the most recent line; fall back to arrival order when untimestamped.
        if best is None or epoch is None or (best_epoch is None or epoch >= best_epoch):
            best, best_epoch = entry, epoch
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


def classify_backend(slots):
    # llama.cpp /slots slots carry is_processing; the gufo engine returns a
    # placeholder payload ({"id","task_id","state",...}) that never changes.
    if not isinstance(slots, list):
        return None
    for slot in slots:
        if not isinstance(slot, dict):
            continue
        if "is_processing" in slot:
            return "llama"
        if "task_id" in slot or "state" in slot:
            return "gufo"
    return None


def fetch_metrics(port: int, timeout=2):
    url = f"http://{HOST}:{port}/metrics"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            text = resp.read().decode("utf-8", "replace")
    except Exception:
        return port, None
    values = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) != 2:
            continue
        try:
            num = float(parts[1])
        except ValueError:
            continue
        if parts[0] == "llamacpp:prompt_tokens_total":
            values["prompt_total"] = int(num)
        elif parts[0] == "llamacpp:tokens_predicted_total":
            values["gen_total"] = int(num)
        elif parts[0] == "llamacpp:prompt_tokens_seconds":
            values["prompt_tps"] = num
        elif parts[0] == "llamacpp:predicted_tokens_seconds":
            values["decode_tps"] = num
    return port, values


def fetch_port_status(port, timeout=2):
    port, slots = fetch_slots(port, timeout)
    backend = classify_backend(slots)
    metrics = None
    if backend == "gufo":
        _, metrics = fetch_metrics(port, timeout)
    return port, backend, slots, metrics


def gufo_activity(state_cache, port, metrics, now_ts, inflight_active):
    # gufo advances its Prometheus counters only when a request completes, so
    # infer the phase from per-poll deltas and replay the last observed phase
    # briefly while a long request keeps running (counters frozen mid-request).
    gen_total = metrics.get("gen_total")
    prompt_total = metrics.get("prompt_total")
    if not isinstance(gen_total, int) or not isinstance(prompt_total, int):
        return None
    prev_key = f"gf_prev:{port}"
    act_key = f"gf_act:{port}"
    prev = state_cache.get(prev_key)
    dgen = dprompt = 0
    if isinstance(prev, dict):
        try:
            dt = now_ts - float(prev.get("ts", now_ts))
            if dt >= 0.05:
                dgen = max(0, gen_total - int(prev.get("gen", gen_total)))
                dprompt = max(0, prompt_total - int(prev.get("prompt", prompt_total)))
        except (TypeError, ValueError):
            pass
    state_cache[prev_key] = {"ts": now_ts, "gen": gen_total, "prompt": prompt_total}

    act = None
    if dgen > 0:
        act = {"kind": "gen", "gen": gen_total, "tps": metrics.get("decode_tps")}
    elif dprompt > 0:
        act = {"kind": "prompt", "gen": gen_total, "tps": metrics.get("prompt_tps")}
    if act is not None:
        state_cache[act_key] = dict(act, ts=now_ts)
        return act
    if inflight_active:
        held = state_cache.get(act_key)
        if (
            isinstance(held, dict)
            and isinstance(held.get("ts"), (int, float))
            and now_ts - float(held["ts"]) <= GUFO_PHASE_HOLD_SECONDS
        ):
            return dict(held, held=True)
    return None


def clear_gufo_state(state_cache, live_ports=None):
    # Keep per-port counter snapshots for ports still running so deltas span
    # idle polls; everything else (replay state, dead ports) is dropped.
    live = {f"gf_prev:{port}" for port in (live_ports or [])}
    for key in [k for k in state_cache if k.startswith("gf_") and k not in live]:
        del state_cache[key]


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
    saw_inflight = False
    got_everything_ts = None
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
                        saw_inflight = True

                    # Connection burst order is logData -> modelStatus ->
                    # inflight snapshot, so never break before the snapshot
                    # arrives; otherwise the busy state stays invisible.
                    if got_model_status and saw_inflight:
                        break
                    if (
                        got_model_status
                        and got_upstream_log
                        and event_count >= 5
                        and time.monotonic() - started >= SSE_INFLIGHT_GRACE_SECONDS
                    ):
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


def inflight_busy_info(inflight_obj):
    # returns (busy, model, elapsed_s, n_requests); llama-swap refreshes
    # elapsed_ms on every inflight event, so it is current as of this poll.
    if not isinstance(inflight_obj, dict):
        return False, None, None, None
    op = inflight_obj.get("operation")
    if op == "remove":
        return False, None, None, None
    if op == "snapshot":
        reqs = inflight_obj.get("requests") or []
        if reqs:
            elapsed = None
            for req in reqs:
                try:
                    ms = float(req.get("elapsed_ms") or 0)
                except (TypeError, ValueError):
                    continue
                if elapsed is None or ms > elapsed:
                    elapsed = ms
            return True, reqs[0].get("model"), int(elapsed / 1000) if elapsed is not None else None, len(reqs)
        return False, None, None, None
    req = inflight_obj.get("request")
    if isinstance(req, dict):
        try:
            ms = float(req.get("elapsed_ms") or 0)
        except (TypeError, ValueError):
            ms = None
        return True, req.get("model"), int(ms / 1000) if ms is not None else None, None
    return False, None, None, None


def fmt_busy(elapsed_s, n_requests):
    if elapsed_s is None:
        base = "Busy"
    elif elapsed_s >= 600:
        base = f"Busy {elapsed_s // 60}m"
    else:
        base = f"Busy {elapsed_s}s"
    if isinstance(n_requests, int) and n_requests > 1:
        base += f" ×{n_requests}"
    return base


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
gufo_ports = {}
log_activity = latest_upstream_activity(upstream_log)
# Authoritative gufo progress (engine >= b722a61 with --log-progress). Wins over
# the Prometheus-delta inference below, which can only guess the phase.
gufo_prog = latest_gufo_progress(upstream_log)
if ports:
    with ThreadPoolExecutor(max_workers=len(ports)) as pool:
        futures = [pool.submit(fetch_port_status, port, 0.25) for port in ports]
        for fut in as_completed(futures, timeout=0.45):
            try:
                port, backend, slots, metrics = fut.result()
            except Exception:
                continue
            if backend == "gufo":
                if metrics:
                    gufo_ports[port] = metrics
                continue
            active = active_slot(slots)
            if active:
                slot_activity = active
                slot_model = ports.get(port)
                break

inflight_active, inflight_model, inflight_elapsed, inflight_n = inflight_busy_info(inflight)

gufo_act = None
gufo_model = None
if not slot_activity and gufo_ports:
    for port, metrics in gufo_ports.items():
        act = gufo_activity(state_cache, port, metrics, now_ts, inflight_active)
        if not act:
            continue
        if gufo_act is None or (gufo_act.get("held") and not act.get("held")):
            gufo_act = act
            gufo_model = ports.get(port)

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
model = slot_model or gufo_model

if not model and inflight_model:
    model = inflight_model

if not model and len(running) == 1:
    model = running[0].get("model") or running[0].get("name")

has_live_activity = bool(slot_activity or gufo_prog or gufo_act or inflight_active)

if has_live_activity:
    if gufo_prog and not slot_activity:
        # Scheduler-authoritative numbers straight from gufo's progress logger.
        tps = gufo_prog.get("tps")
        if gufo_prog.get("kind") == "gen":
            state_cache.pop("prompt_pct", None)
            state_cache.pop("prompt_ts", None)
            parts = [f"⚡ Gen {gufo_prog.get('done', 0)}t"]
            if isinstance(tps, (int, float)) and tps > 0.05:
                parts.append(f"{tps:.1f} tok/s")
            acc = gufo_prog.get("acceptance_pct")
            if isinstance(acc, (int, float)):
                parts.append(f"{acc:.0f}% MTP")
            label = " ".join(parts)
        else:
            pct = gufo_prog.get("pct")
            if isinstance(pct, (int, float)):
                label = f"🟡 Prompt {pct:.0f}%"
                if isinstance(tps, (int, float)) and tps > 0.05:
                    label += f" {tps:.0f} tok/s"
            elif isinstance(tps, (int, float)) and tps > 0.05:
                label = f"🟡 Prompt {tps:.0f} tok/s"
            else:
                label = "🟡 Prompt"
    elif gufo_act and not slot_activity:
        tps = gufo_act.get("tps")
        if gufo_act.get("kind") == "gen":
            state_cache.pop("prompt_pct", None)
            state_cache.pop("prompt_ts", None)
            gen_total = gufo_act.get("gen", 0)
            if isinstance(tps, (int, float)) and tps > 0.05:
                label = f"⚡ Gen {gen_total}t {tps:.1f} tok/s"
            else:
                label = f"⚡ Gen {gen_total}t"
        elif isinstance(tps, (int, float)) and tps > 0.05:
            label = f"🟡 Prompt {tps:.0f} tok/s"
        else:
            label = "🟡 Prompt"
    else:
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
            label = f"🟡 {fmt_busy(inflight_elapsed, inflight_n)}"
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
    clear_stale_state()
    clear_gufo_state(state_cache, gufo_ports)
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
    clear_stale_state()
    clear_gufo_state(state_cache, gufo_ports)
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
    clear_stale_state()
    clear_gufo_state(state_cache, gufo_ports)
    state_cache["last_phase"] = "idle"
    state_cache["last_line"] = out_line
    state_cache["last_ok_ts"] = now_ts
    state_cache["last_loaded"] = loaded
    if total is not None:
        state_cache["last_total"] = total
    save_state(state_cache)
PY
