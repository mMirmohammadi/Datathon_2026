#!/usr/bin/env bash
# Cleanly stop the demo server + tunnel. Reads PIDs from /tmp/datathon_logs.
set -u

LOG_DIR="${LOG_DIR:-/tmp/datathon_logs}"

for name in uvicorn ngrok; do
    pid_file="${LOG_DIR}/${name}.pid"
    if [[ -f "${pid_file}" ]]; then
        pid=$(cat "${pid_file}" 2>/dev/null || true)
        if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
            echo "stopping ${name} pid=${pid}"
            kill -TERM "${pid}" 2>/dev/null || true
        fi
        rm -f "${pid_file}"
    fi
done

# Belt-and-braces: also kill the supervisor wrappers themselves so they don't
# relaunch the children.
pkill -TERM -f 'scripts/deploy/run_server.sh' 2>/dev/null || true
pkill -TERM -f 'scripts/deploy/run_tunnel.sh' 2>/dev/null || true

sleep 1

# Second pass: anything still listening on the app port is killed hard.
if command -v lsof >/dev/null 2>&1; then
    lsof -ti ":${LISTINGS_PORT:-8000}" 2>/dev/null | xargs -r kill -KILL 2>/dev/null || true
fi
pkill -KILL -f 'ngrok http' 2>/dev/null || true
pkill -KILL -f 'uvicorn app.main:app' 2>/dev/null || true

echo "stopped."
