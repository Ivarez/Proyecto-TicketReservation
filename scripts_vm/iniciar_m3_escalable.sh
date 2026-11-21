#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

python3 persistencia.py --rol lider --reset --modo-repl semi-sincrono --puerto 5557 --db srb_lider.db "$@" &
P_LIDER=$!
sleep 1
python3 persistencia.py --rol seguidor --id seg1 --reset --puerto 5567 --db srb_seg1.db --lider-host 127.0.0.1 &
P_SEG1=$!
python3 persistencia.py --rol seguidor --id seg2 --reset --puerto 5568 --db srb_seg2.db --lider-host 127.0.0.1 &
P_SEG2=$!

echo "[M3-ESCALABLE] PIDs -> Lider=$P_LIDER Seg1=$P_SEG1 Seg2=$P_SEG2"
trap 'kill $P_LIDER $P_SEG1 $P_SEG2 2>/dev/null || true' EXIT INT TERM
wait
