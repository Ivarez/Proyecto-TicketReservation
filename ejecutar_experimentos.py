"""Orquestador automatizado de los 6 escenarios de rendimiento (Tabla 1 del Enunciado).

Compara **Arquitectura Base** (1 BD sin réplicas, 1 GT, 1 RMC) frente a **Arquitectura Escalable**
(1 BD Líder + 2 BD Seguidoras en lectura, 2 GTs Primario/Backup, 4 RMCs y 2 Cancelaciones)
para 3, 5 y 8 clientes simultáneos con reinicio limpio de la BD entre cada escenario.
"""
import argparse
import os
import shutil
import socket
import subprocess
import sys
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def esperar_puerto_tcp(port: int, timeout: float = 8.0) -> None:
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"Puerto TCP {port} no abrió en {timeout}s")


def env_experimento(carpeta: str) -> dict:
    env = os.environ.copy()
    env.update(
        {
            "SRB_GT_HOST": "127.0.0.1",
            "SRB_GT_BACKUP_HOST": "127.0.0.1",
            "SRB_PERSIST_HOST": "127.0.0.1",
            "SRB_PERSIST_REPLICAS": "127.0.0.1:5567,127.0.0.1:5568",
            "SRB_DB_PATH": os.path.join(carpeta, "srb_lider.db"),
            "SRB_MODO_REPL": "semi-sincrono",
        }
    )
    return env


def lanzar(script: str, args: list[str], env: dict, cwd: str) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, os.path.join(BASE_DIR, script), *args],
        env=env,
        cwd=cwd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def detener(procs: list[subprocess.Popen]) -> None:
    for p in procs:
        if p.poll() is None:
            p.terminate()
    for p in procs:
        if p.poll() is None:
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()


def clientes_tabla1(num_clientes: int) -> list[dict]:
    """Configuración exacta de roles según la Tabla 1 del enunciado."""
    if num_clientes == 3:
        return [
            {"id": "C1", "c": 100, "r": 0, "m": 0},
            {"id": "C2", "c": 100, "r": 0, "m": 0},
            {"id": "C3", "c": 50, "r": 50, "m": 0},
        ]
    if num_clientes == 5:
        return [
            {"id": "C1", "c": 100, "r": 0, "m": 0},
            {"id": "C2", "c": 100, "r": 0, "m": 0},
            {"id": "C3", "c": 100, "r": 0, "m": 0},
            {"id": "C4", "c": 0, "r": 60, "m": 40},
            {"id": "C5", "c": 0, "r": 60, "m": 40},
        ]
    if num_clientes == 8:
        return (
            [{"id": f"C{i}", "c": 100, "r": 0, "m": 0} for i in range(1, 5)]
            + [{"id": f"C{i}", "c": 0, "r": 60, "m": 40} for i in range(5, 9)]
        )
    raise ValueError(f"Escenario no contemplado en Tabla 1: {num_clientes} clientes")


def correr_escenario(
    arq: str,
    num_clientes: int,
    duracion: int,
    rpm: float,
    pausa_ms: float,
    salida_root: str,
    sim_trabajo_ms: float = 0.0,
) -> None:
    nombre = f"{arq}_{num_clientes}c"
    carpeta = os.path.join(salida_root, nombre)
    shutil.rmtree(carpeta, ignore_errors=True)
    os.makedirs(carpeta, exist_ok=True)
    env = env_experimento(carpeta)
    if sim_trabajo_ms > 0:
        env["SRB_BENCH_WORK_MS"] = str(sim_trabajo_ms)

    print(f"\n=== Escenario {nombre} | duracion={duracion}s | rpm={rpm} | pausa_ms={pausa_ms} ===", flush=True)
    infra: list[subprocess.Popen] = []
    try:
        if arq == "base":
            env["SRB_PERSIST_REPLICAS"] = ""
            infra.append(
                lanzar(
                    "persistencia.py",
                    ["--rol", "lider", "--reset", "--modo-repl", "none", "--db", env["SRB_DB_PATH"], "--metricas", os.path.join(carpeta, "persist_lider.csv")],
                    env,
                    carpeta,
                )
            )
            esperar_puerto_tcp(5557)
            infra.append(
                lanzar(
                    "gestor_transacciones.py",
                    ["--rol", "primario", "--metricas", os.path.join(carpeta, "gt_metricas_primario.csv")],
                    env,
                    carpeta,
                )
            )
            esperar_puerto_tcp(5555)
            esperar_puerto_tcp(5556)
            esperar_puerto_tcp(5559)
            infra.append(
                lanzar("rmc.py", ["--worker-id", "rmc-base", "--sin-replicas", "--solo-primario"], env, carpeta)
            )
            infra.append(lanzar("cancelaciones.py", ["--worker-id", "cancel-base", "--solo-primario"], env, carpeta))
        else:
            infra.append(
                lanzar(
                    "persistencia.py",
                    ["--rol", "lider", "--reset", "--db", env["SRB_DB_PATH"], "--metricas", os.path.join(carpeta, "persist_lider.csv")],
                    env,
                    carpeta,
                )
            )
            esperar_puerto_tcp(5557)
            esperar_puerto_tcp(5565)
            esperar_puerto_tcp(5570)
            infra.append(
                lanzar(
                    "persistencia.py",
                    ["--rol", "seguidor", "--id", "seg1", "--reset", "--puerto", "5567", "--db", os.path.join(carpeta, "srb_seg1.db"), "--metricas", os.path.join(carpeta, "persist_seg1.csv")],
                    env,
                    carpeta,
                )
            )
            infra.append(
                lanzar(
                    "persistencia.py",
                    ["--rol", "seguidor", "--id", "seg2", "--reset", "--puerto", "5568", "--db", os.path.join(carpeta, "srb_seg2.db"), "--metricas", os.path.join(carpeta, "persist_seg2.csv")],
                    env,
                    carpeta,
                )
            )
            esperar_puerto_tcp(5567)
            esperar_puerto_tcp(5568)
            infra.append(
                lanzar(
                    "gestor_transacciones.py",
                    ["--rol", "primario", "--metricas", os.path.join(carpeta, "gt_metricas_primario.csv")],
                    env,
                    carpeta,
                )
            )
            esperar_puerto_tcp(5555)
            esperar_puerto_tcp(5556)
            esperar_puerto_tcp(5559)
            infra.append(
                lanzar(
                    "gestor_transacciones.py",
                    ["--rol", "backup", "--metricas", os.path.join(carpeta, "gt_metricas_backup.csv")],
                    env,
                    carpeta,
                )
            )
            esperar_puerto_tcp(5659)
            infra.append(lanzar("supervisor.py", ["--rmc", "4", "--cancel", "2"], env, carpeta))

        time.sleep(0.8)

        specs = clientes_tabla1(num_clientes)
        procs_clientes: list[subprocess.Popen] = []
        for idx, c in enumerate(specs):
            cli_args = [
                "--cliente-id", c["id"],
                "--duracion", str(duracion),
                "--rpm", str(rpm),
                "--pausa-ms", str(pausa_ms),
                "--pct-consulta", str(c["c"]),
                "--pct-reserva", str(c["r"]),
                "--pct-modificacion", str(c["m"]),
                "--pct-cancelacion", "0",
                "--max-asientos", "2",
                "--semilla", str(100 + idx),
                "--salida", os.path.join(carpeta, f"resultados_{c['id']}.csv"),
            ]
            if arq == "base":
                cli_args.append("--solo-primario")
            procs_clientes.append(lanzar("cliente_carga.py", cli_args, env, carpeta))

        for p in procs_clientes:
            p.wait()
        print(f"  -> Finalizado {nombre} en {carpeta}", flush=True)
    finally:
        detener(infra)
        time.sleep(0.5)


def main() -> None:
    ap = argparse.ArgumentParser(description="Ejecutor de experimentos Tabla 1 SRB")
    ap.add_argument("--duracion", type=int, default=150, help="Duración por escenario (s)")
    ap.add_argument("--rpm", type=float, default=0, help="Solicitudes/min por cliente (0 = carga continua)")
    ap.add_argument("--pausa-ms", type=float, default=5.0, help="Pausa entre peticiones cuando --rpm 0")
    ap.add_argument("--salida-dir", default="resultados_experimentos")
    ap.add_argument("--solo-arq", choices=["base", "escalable", "ambas"], default="ambas")
    args = ap.parse_args()

    salida_root = os.path.abspath(os.path.join(BASE_DIR, args.salida_dir))
    os.makedirs(salida_root, exist_ok=True)

    arqs = ["base", "escalable"] if args.solo_arq == "ambas" else [args.solo_arq]
    for arq in arqs:
        for n in (3, 5, 8):
            correr_escenario(arq, n, args.duracion, args.rpm, args.pausa_ms, salida_root)

    print(f"\n=== Experimentos completados en {salida_root} ===", flush=True)


if __name__ == "__main__":
    main()
