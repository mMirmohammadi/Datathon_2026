#!/usr/bin/env bash
# Production-ish uvicorn wrapper for the Datathon 2026 demo.
#
# Keeps uvicorn restarting forever if it crashes (backoff), writes rotating
# logs, turns off the noisy access log, and caps concurrency so a Slashdot
# moment can't OOM the box.
#
# One worker on purpose: SigLIP-2 Giant (~7 GB VRAM) and the DINOv2 store
# (~300 MB mmap) are per-process, and the RTX 5090 has only 32 GB of VRAM.
# Multiple workers would each warm up their own copy + contend for GPU SMs.
# FastAPI's thread-pool (default anyio, 40 threads on this box) handles
# concurrent requests from the single worker just fine.

set -u  # -e removed on purpose: we WANT the loop to keep going on crash

LOG_DIR="${LOG_DIR:-/tmp/datathon_logs}"
LOG_FILE="${LOG_DIR}/uvicorn.log"
PID_FILE="${LOG_DIR}/uvicorn.pid"
HOST="${LISTINGS_HOST:-127.0.0.1}"
PORT="${LISTINGS_PORT:-8000}"
BACKOFF_START=2
BACKOFF_MAX=60
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

mkdir -p "${LOG_DIR}"

# shellcheck disable=SC1091
source /home/rohamzn/miniconda3/etc/profile.d/conda.sh
conda activate datathon2026

cd "${REPO_DIR}"

: "${LISTINGS_VISUAL_ENABLED:=1}"
: "${LISTINGS_TEXT_EMBED_ENABLED:=1}"
: "${LISTINGS_DINOV2_ENABLED:=1}"
export LISTINGS_VISUAL_ENABLED LISTINGS_TEXT_EMBED_ENABLED LISTINGS_DINOV2_ENABLED

backoff="${BACKOFF_START}"

while true; do
    # Rotate the log: if it's > 100 MB, move to .1
    if [[ -f "${LOG_FILE}" ]] && [[ $(stat -c%s "${LOG_FILE}" 2>/dev/null || echo 0) -gt 104857600 ]]; then
        mv "${LOG_FILE}" "${LOG_FILE}.1"
    fi
    echo "[$(date -u +%FT%TZ)] launching uvicorn on ${HOST}:${PORT}" >> "${LOG_FILE}"
    # The production flags:
    #   --no-access-log            -> stop per-request log spam on a busy tunnel
    #   --timeout-keep-alive 75    -> survive ngrok/cloudflare keep-alive probes
    #   --limit-concurrency 128    -> soft-fail at 128 concurrent (503 after)
    #   --log-level warning        -> only real problems in the log
    python -m uvicorn app.main:app \
        --host "${HOST}" --port "${PORT}" \
        --workers 1 \
        --no-access-log \
        --timeout-keep-alive 75 \
        --limit-concurrency 128 \
        --log-level warning \
        >> "${LOG_FILE}" 2>&1 &

    echo $! > "${PID_FILE}"
    wait "$!"
    exit_code=$?

    echo "[$(date -u +%FT%TZ)] uvicorn exited code=${exit_code}; restarting in ${backoff}s" \
        >> "${LOG_FILE}"
    sleep "${backoff}"
    # Exponential backoff, capped. Resets on next clean minute of uptime.
    backoff=$((backoff * 2))
    if (( backoff > BACKOFF_MAX )); then backoff="${BACKOFF_MAX}"; fi
done
