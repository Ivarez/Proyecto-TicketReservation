#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

export SRB_PERSIST_HOST="${1:-${SRB_PERSIST_HOST:-127.0.0.1}}"
export SRB_PERSIST_REPLICAS="${SRB_PERSIST_REPLICAS:-${SRB_PERSIST_HOST}:5567,${SRB_PERSIST_HOST}:5568}"
export SRB_GT_HOST="${2:-${SRB_GT_HOST:-127.0.0.1}}"
export SRB_GT_BACKUP_HOST="${SRB_GT_BACKUP_HOST:-$SRB_GT_HOST}"

python3 gestor_transacciones.py --rol primario &
P_GT1=$!
python3 gestor_transacciones.py --rol backup &
P_GT2=$!
sleep 1
python3 supervisor.py --rmc 4 --cancel 2 &
P_SUP=$!

echo "[M2-ESCALABLE] PERSIST=$SRB_PERSIST_HOST | REPLICAS=$SRB_PERSIST_REPLICAS"
echo "[M2-ESCALABLE] PIDs -> GT_PRIMARIO=$P_GT1 GT_BACKUP=$P_GT2 SUPERVISOR=$P_SUP"
trap 'kill $P_GT1 $P_GT2 $P_SUP 2>/dev/null || true' EXIT INT TERM
wait
