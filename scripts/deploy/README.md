# Demo deploy (qolam → public URL via ngrok)

Three tiny shell scripts + one `ngrok` binary at `~/.local/bin/ngrok`.
No sudo, no systemd, no Docker. Survives crashes; survives ngrok drops.

## Start

```bash
cd ~/ETH_Uni/Datathon_2026
nohup ./scripts/deploy/run_server.sh >/dev/null 2>&1 & disown
nohup ./scripts/deploy/run_tunnel.sh >/dev/null 2>&1 & disown
```

First startup is ~45 s (SigLIP 7 GB weights warm on GPU + DINOv2 + Arctic
indexes). The tunnel comes up in ~2 s on top of that.

## Check

```bash
cat /tmp/datathon_logs/public_url.txt
curl -s "$(cat /tmp/datathon_logs/public_url.txt)/health"
```

The two supervisor processes (`run_server.sh`, `run_tunnel.sh`) and their
children are all backgrounded with `disown`, so closing the terminal does
not kill them. Logs stream to `/tmp/datathon_logs/`:

- `uvicorn.log`      — server errors + warnings (access log off for perf)
- `uvicorn.log.1`    — rotated at 100 MB
- `ngrok.log`        — tunnel events (JSON per line)
- `public_url.txt`   — current public URL (overwritten per reconnect)

## Stop

```bash
./scripts/deploy/stop.sh
```

Terminates the supervisors + both children. Safe to re-run.

## Production flags in use

`uvicorn app.main:app --workers 1 --no-access-log --timeout-keep-alive 75
--limit-concurrency 128 --log-level warning`

Why 1 worker: SigLIP-2 Giant (7 GB VRAM) + DINOv2 (290 MB mmap) + Arctic
(100 MB RAM) are per-process. Two workers would each warm up their own
copies and fight the RTX 5090 for SMs. FastAPI's sync-handler threadpool
(40 threads default on this 24-core box) plus a single-GPU queue is the
right concurrency model — see the observed tail-latency table below.

## Observed behaviour under load

8 parallel text queries from a cold process, HTTP 200 across the board:

| requests | p50  | p95  | tail |
|---------:|-----:|-----:|-----:|
| 1        | 2.5s | 2.5s | 2.5s |
| 8        | 10s  | 17s  | 19s  |

Bottleneck is the GPU (SigLIP text-encode + DINOv2) + one OpenAI call per
query for the hard-fact extractor. Stakeholder demo is fine at ≤ 5
simultaneous users; beyond that, queue behaviour starts to show.

## Auto-restart behaviour

Both supervisors use exponential backoff (2 s → 4 s → ... capped at 60 s)
when their child crashes. After a clean minute of uptime the backoff
resets. This is intentional — we'd rather a crash-looping process slow
down than saturate the OOM-killer queue.

## Security notes (what ngrok exposes)

The tunnel bridges the public internet straight into `127.0.0.1:8000`.
Everything served by the FastAPI app is reachable, including:

- `data/listings.db` via search queries (read-only; public-corpus data).
- `data/users.db` via `/auth/*` endpoints (argon2id hashed passwords,
  HttpOnly session cookies, CSRF double-submit). Safe to expose, but
  rotate the session secret if you don't trust the tunnel URL:
  ```bash
  export LISTINGS_SESSION_SECRET=$(head -c 32 /dev/urandom | base64)
  ./scripts/deploy/stop.sh
  nohup ./scripts/deploy/run_server.sh >/dev/null 2>&1 & disown
  ```

Rate-limiting is in-process (10 login failures / 5 min per username). An
external IP-based rate limit should be added if this ever stays up beyond
a demo window.

## Troubleshooting

**URL returns 502** → uvicorn is restarting. Check `tail -f
/tmp/datathon_logs/uvicorn.log`; wait ~45 s for the SigLIP warm.

**`ERR_NGROK_8012`** → tunnel died, supervisor is respawning. New URL
lands in `public_url.txt` within a couple of seconds; your old link won't
work — tell people to refresh.

**First query after idle is slow** → HF Hub reconnects + DINOv2 model is
lazy-loaded on first upload. Pre-warm by running a text search + an image
upload once before the demo.
