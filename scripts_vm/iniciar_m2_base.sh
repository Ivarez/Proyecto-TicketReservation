#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

export SRB_PERSIST_HOST="${1:-${SRB_PERSIST_HOST:-127.0.0.1}}"
export SRB_PERSIST_REPLICAS=""

python3 gestor_transacciones.py --rol primario &
P_GT=$!
sleep 1
python3 rmc.py --worker-id rmc-base --sin-replicas --solo-primario &
P_RMC=$!
python3 cancelaciones.py --worker-id cancel-base --solo-primario &
P_CAN=$!

echo "[M2-BASE] PERSIST=$SRB_PERSIST_HOST | PIDs -> GT=$P_GT RMC=$P_RMC CANCEL=$P_CAN"
trap 'kill $P_GT $P_RMC $P_CAN 2>/dev/null || true' EXIT INT TERM
wait
