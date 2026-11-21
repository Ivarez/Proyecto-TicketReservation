#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ $# -lt 3 ]]; then
  echo "Uso: $0 <IP_M2> <3|5|8> <base|escalable> [duracion_s=150] [rpm=0]"
  exit 1
fi

export SRB_GT_HOST="$1"
export SRB_GT_BACKUP_HOST="${SRB_GT_BACKUP_HOST:-$1}"
ESCENARIO="$2"
ARQ="$3"
DURACION="${4:-150}"
RPM="${5:-0}"
OUT_DIR="resultados_vm_${ARQ}_${ESCENARIO}c"
mkdir -p "$OUT_DIR"

EXTRA=""
if [[ "$ARQ" == "base" ]]; then
  EXTRA="--solo-primario"
fi

PIDS=()
lanzar_cli() {
  local cid="$1"; shift
  python3 cliente_carga.py --cliente-id "$cid" --duracion "$DURACION" --rpm "$RPM" --pausa-ms 5 \
    --salida "$OUT_DIR/resultados_${cid}.csv" $EXTRA "$@" &
  PIDS+=($!)
}

case "$ESCENARIO" in
  3)
    lanzar_cli C1 --pct-consulta 100
    lanzar_cli C2 --pct-consulta 100
    lanzar_cli C3 --pct-consulta 50 --pct-reserva 50
    ;;
  5)
    lanzar_cli C1 --pct-consulta 100
    lanzar_cli C2 --pct-consulta 100
    lanzar_cli C3 --pct-consulta 100
    lanzar_cli C4 --pct-consulta 0 --pct-reserva 60 --pct-modificacion 40
    lanzar_cli C5 --pct-consulta 0 --pct-reserva 60 --pct-modificacion 40
    ;;
  8)
    for i in 1 2 3 4; do
      lanzar_cli "C$i" --pct-consulta 100
    done
    for i in 5 6 7 8; do
      lanzar_cli "C$i" --pct-consulta 0 --pct-reserva 60 --pct-modificacion 40
    done
    ;;
  *)
    echo "Escenario inválido: $ESCENARIO (use 3, 5 u 8)"
    exit 1
    ;;
esac

echo "[M1] Ejecutando escenario $ESCENARIO clientes ($ARQ) hacia $SRB_GT_HOST durante ${DURACION}s..."
for p in "${PIDS[@]}"; do
  wait "$p"
done

python3 analisis.py "$OUT_DIR/resultados_*.csv" --warmup 15
