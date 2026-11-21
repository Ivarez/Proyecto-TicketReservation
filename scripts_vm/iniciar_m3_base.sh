#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 persistencia.py --rol lider --reset --modo-repl none --puerto 5557 --db srb_lider.db "$@"
