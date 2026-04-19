#!/usr/bin/env bash
# ngrok supervisor — keeps the public tunnel alive through transient drops.
#
# After the tunnel starts, the public URL can be fetched from the local ngrok
# API at http://127.0.0.1:4040/api/tunnels (what `ngrok api tunnels list` reads).

set -u

LOG_DIR="${LOG_DIR:-/tmp/datathon_logs}"
LOG_FILE="${LOG_DIR}/ngrok.log"
PID_FILE="${LOG_DIR}/ngrok.pid"
URL_FILE="${LOG_DIR}/public_url.txt"
LOCAL_PORT="${LISTINGS_PORT:-8000}"
BACKOFF_START=2
BACKOFF_MAX=60

mkdir -p "${LOG_DIR}"

export PATH="${HOME}/.local/bin:${PATH}"

backoff="${BACKOFF_START}"

while true; do
    echo "[$(date -u +%FT%TZ)] launching ngrok http ${LOCAL_PORT}" >> "${LOG_FILE}"
    # --log stdout so our wrapper gets everything; ngrok writes structured logs
    # that are easy to grep.
    ngrok http "${LOCAL_PORT}" \
        --log stdout \
        --log-format json \
        >> "${LOG_FILE}" 2>&1 &

    pid=$!
    echo "${pid}" > "${PID_FILE}"

    # Poll the local ngrok API (starts after a second or two) for the public URL
    # and persist it for other scripts to read.
    for _ in 1 2 3 4 5 6 7 8 9 10; do
        sleep 1
        url=$(curl -sS http://127.0.0.1:4040/api/tunnels 2>/dev/null \
              | python3 -c 'import json,sys
try:
    d = json.load(sys.stdin)
    for t in d.get("tunnels", []):
        if t.get("proto") == "https":
            print(t["public_url"]); break
except Exception:
    pass' 2>/dev/null)
        if [[ -n "${url}" ]]; then
            echo "${url}" > "${URL_FILE}"
            echo "[$(date -u +%FT%TZ)] public URL: ${url}" >> "${LOG_FILE}"
            break
        fi
    done

    wait "${pid}"
    exit_code=$?
    echo "[$(date -u +%FT%TZ)] ngrok exited code=${exit_code}; restarting in ${backoff}s" \
        >> "${LOG_FILE}"
    rm -f "${URL_FILE}"
    sleep "${backoff}"
    backoff=$((backoff * 2))
    if (( backoff > BACKOFF_MAX )); then backoff="${BACKOFF_MAX}"; fi
done
