"""Supervisor / Watchdog de Reconciliación (``Want == Got``) para la Capa de Negocio.

Inspirado en el bucle de reconciliación de controladores (ReplicaSet / kubelet):
mantiene el número deseado de procesos ``rmc.py`` y ``cancelaciones.py`` activos en M2,
detecta caídas (ej. ``kill -9`` en sustentación) cada ``--intervalo`` segundos y lanza
automáticamente un proceso sustituto para preservar disponibilidad y capacidad.
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def lanzar_proceso(script: str, extra_args: list[str]) -> subprocess.Popen:
    cmd = [sys.executable, os.path.join(BASE_DIR, script), *extra_args]
    return subprocess.Popen(cmd)


def escribir_estado(ruta: str | None, procesos: list[tuple[dict, subprocess.Popen]]) -> None:
    if not ruta:
        return
    data = {esp["nombre"]: proc.pid for esp, proc in procesos}
    tmp = ruta + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp, ruta)


def main() -> None:
    ap = argparse.ArgumentParser(description="Supervisor Watchdog SRB (Capa de Negocio)")
    ap.add_argument("--rmc", type=int, default=4, help="Número deseado de workers RMC")
    ap.add_argument("--cancel", type=int, default=2, help="Número deseado de workers Cancelaciones")
    ap.add_argument("--con-gt", action="store_true", help="Supervisar también GT Primario y Backup")
    ap.add_argument("--sin-replicas", action="store_true", help="Pasar --sin-replicas a los RMC")
    ap.add_argument("--solo-primario", action="store_true", help="Pasar --solo-primario a workers")
    ap.add_argument("--intervalo", type=float, default=0.3, help="Intervalo de sondeo en segundos")
    ap.add_argument("--estado-file", default=None, help="Archivo JSON opcional con PIDs activos")
    args = ap.parse_args()

    especificaciones: list[dict] = []
    if args.con_gt:
        especificaciones.append(
            {"nombre": "gt-primario", "script": "gestor_transacciones.py", "args": ["--rol", "primario"]}
        )
        especificaciones.append(
            {"nombre": "gt-backup", "script": "gestor_transacciones.py", "args": ["--rol", "backup"]}
        )

    for i in range(1, args.rmc + 1):
        rmc_args = ["--worker-id", f"rmc-{i}"]
        if args.sin_replicas:
            rmc_args.append("--sin-replicas")
        if args.solo_primario:
            rmc_args.append("--solo-primario")
        especificaciones.append({"nombre": f"rmc-{i}", "script": "rmc.py", "args": rmc_args})

    for i in range(1, args.cancel + 1):
        c_args = ["--worker-id", f"cancel-{i}"]
        if args.solo_primario:
            c_args.append("--solo-primario")
        especificaciones.append({"nombre": f"cancel-{i}", "script": "cancelaciones.py", "args": c_args})

    procesos: list[tuple[dict, subprocess.Popen]] = []
    for esp in especificaciones:
        p = lanzar_proceso(esp["script"], esp["args"])
        procesos.append((esp, p))
        print(f"[SUPERVISOR] Iniciado {esp['nombre']} (PID={p.pid})", flush=True)
    escribir_estado(args.estado_file, procesos)

    ejecutando = True

    def _parar(*_):
        nonlocal ejecutando
        ejecutando = False

    signal.signal(signal.SIGINT, _parar)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _parar)

    try:
        while ejecutando:
            time.sleep(args.intervalo)
            hubo_cambio = False
            for idx, (esp, proc) in enumerate(procesos):
                rc = proc.poll()
                if rc is not None and ejecutando:
                    print(
                        f"[SUPERVISOR] ¡Proceso {esp['nombre']} (PID={proc.pid}) murió (rc={rc})! "
                        "Reconciliando (Want == Got) -> lanzando reemplazo...",
                        flush=True,
                    )
                    nuevo = lanzar_proceso(esp["script"], esp["args"])
                    procesos[idx] = (esp, nuevo)
                    hubo_cambio = True
                    print(
                        f"[SUPERVISOR] Reemplazo {esp['nombre']} activo (PID={nuevo.pid})",
                        flush=True,
                    )
            if hubo_cambio:
                escribir_estado(args.estado_file, procesos)
    finally:
        print("[SUPERVISOR] Deteniendo todos los procesos supervisados...", flush=True)
        for _, proc in procesos:
            if proc.poll() is None:
                proc.terminate()
        for _, proc in procesos:
            if proc.poll() is None:
                try:
                    proc.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    proc.kill()


if __name__ == "__main__":
    main()
