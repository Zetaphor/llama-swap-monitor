# llama-swap-monitor

One-line status output for [llama-swap](https://github.com/mostlygeek/llama-swap), aimed at KDE’s **Command Output** panel widget (or any script that polls stdout).

## Example output

```
🟢 Idle · 3 loaded / 45
🟡 Prompt 78% · qwen3.6-35b-a3b-coding
⚡ Gen 142t · qwen3.5-4b
🟡 Starting · qwen3.6-27b
🔴 Offline
```

## Requirements

- Bash
- Python 3 (stdlib only)
- Network access to your llama-swap HTTP API (default port **10080**)

## Setup

```bash
chmod +x llama-swap-monitor.sh
mkdir -p ~/.config/llama-swap-monitor
printf '%s\n' 'LLAMA_SWAP_API=http://192.168.50.240:10080' > ~/.config/llama-swap-monitor/config
./llama-swap-monitor.sh
```

KDE panel widgets run with a minimal environment (no shell profile), so use the config file above rather than relying on `export` in `.bashrc`.

### KDE Command Output widget

1. Add a **Command Output** widget to your panel.
2. Command: full path to `llama-swap-monitor.sh` (or a wrapper that sets `LLAMA_SWAP_API`).
3. Refresh interval: **2–4 seconds** (use 3–4s if models are under heavy load; polls can take a few seconds while prompting).

Example wrapper in `~/.config/environment.d/llama-swap.conf` (Plasma) or your shell profile:

```bash
export LLAMA_SWAP_API=http://192.168.50.240:10080
```

## Configuration

| Source | Description |
|--------|-------------|
| `~/.config/llama-swap-monitor/config` | Shell snippet sourced by the script (recommended for KDE) |
| `LLAMA_SWAP_API` env var | Overrides config file if set |

| Variable | Default | Description |
|----------|---------|-------------|
| `LLAMA_SWAP_API` | `http://127.0.0.1:10080` | Base URL for llama-swap (no trailing slash) |

The script reads the hostname from that URL when querying llama-server `/slots` on backend ports listed in `/running`.

## How it works

llama-swap no longer exposes the old `/api/status` endpoint. This script instead:

1. Checks `/api/version` and `/running`.
2. Reads the upstream log tail from `/api/events` (SSE) to detect **prompt processing** progress and **generation** token counts—the same lines shown in the llama-swap UI logs.
3. Optionally queries each loaded backend’s `/slots` API for `is_processing` when the server responds in time.

The `inflight` SSE counter is not used; it stays at zero during long streamed completions.

## License

MIT
