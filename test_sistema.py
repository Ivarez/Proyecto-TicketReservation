"""Suite de pruebas automatizadas end-to-end para el SRB (Entrega 2).

Verifica:
1. Funcionalidad completa (Consulta, Reserva, Modificación de asientos y fecha, Cancelación asíncrona + notificación).
2. Concurrencia estricta (Strict 2PL contra sobreventa, idempotencia por id_tx y validación optimista por versión).
3. Replicación Líder-Seguidor, consistencia Read-Your-Writes y recuperación Catch-Up.
4. Tolerancia a fallos de RMC (kill de worker en vivo + reenvío en GT + reemplazo por Supervisor).
5. Tolerancia a fallos de GT (kill de GT Primario bajo carga -> promoción de GT Backup + continuidad transparente).
"""
import importlib
import json
import os
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import uuid

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TMP_ROOT = os.path.join(BASE_DIR, "tmp_test")


def env_suite(base_port: int, tmpdir: str, extra: dict | None = None) -> dict:
    """Genera variables de entorno aisladas en puertos únicos para una suite."""
    env = os.environ.copy()
    env.update(
        {
            "SRB_GT_HOST": "127.0.0.1",
            "SRB_GT_BACKUP_HOST": "127.0.0.1",
            "SRB_PERSIST_HOST": "127.0.0.1",
            "SRB_GT_FRONT_PORT": str(base_port),
            "SRB_GT_BACK_PORT": str(base_port + 1),
            "SRB_GT_ASYNC_PORT": str(base_port + 2),
            "SRB_GT_CANCEL_PUSH_PORT": str(base_port + 3),
            "SRB_GT_HB_PORT": str(base_port + 4),
            "SRB_GT_BACKUP_FRONT_PORT": str(base_port + 10),
            "SRB_GT_BACKUP_BACK_PORT": str(base_port + 11),
            "SRB_GT_BACKUP_ASYNC_PORT": str(base_port + 12),
            "SRB_GT_BACKUP_CANCEL_PUSH_PORT": str(base_port + 13),
            "SRB_PERSIST_PORT": str(base_port + 20),
            "SRB_PERSIST_REPL_PORT": str(base_port + 21),
            "SRB_PERSIST_CATCHUP_PORT": str(base_port + 22),
            "SRB_PERSIST_HB_PORT": str(base_port + 23),
            "SRB_PERSIST_REPLICAS": f"127.0.0.1:{base_port + 24},127.0.0.1:{base_port + 25}",
            "SRB_DB_PATH": os.path.join(tmpdir, "lider.db"),
            "SRB_MODO_REPL": "semi-sincrono",
            "SRB_CLIENT_TIMEOUT_MS": "2000",
            "SRB_GT_FAILOVER_TIMEOUT_S": "1.2",
            "SRB_GT_WORKER_EXPIRY_S": "1.5",
            "SRB_GT_TASK_TIMEOUT_S": "1.2",
        }
    )
    if extra:
        env.update(extra)
    return env


def lanzar(script: str, args: list[str], env: dict, log_path: str | None = None) -> subprocess.Popen:
    stdout = open(log_path, "w", encoding="utf-8") if log_path else subprocess.DEVNULL
    return subprocess.Popen(
        [sys.executable, os.path.join(BASE_DIR, script), *args],
        env=env,
        cwd=BASE_DIR,
        stdout=stdout,
        stderr=subprocess.STDOUT if log_path else subprocess.DEVNULL,
    )


def detener_todos(procs: list[subprocess.Popen]) -> None:
    for p in procs:
        if p.poll() is None:
            p.terminate()
    for p in procs:
        if p.poll() is None:
            try:
                p.wait(timeout=2)
            except subprocess.TimeoutExpired:
                p.kill()


def cargar_cliente_con_env(env: dict):
    """Recarga config y cliente_carga para usar los puertos de la suite en proceso actual."""
    for k, v in env.items():
        if k.startswith("SRB_"):
            os.environ[k] = v
    import config
    import cliente_carga

    importlib.reload(config)
    importlib.reload(cliente_carga)
    return cliente_carga.ClienteSRB


def esperar_puerto_tcp(port: int, timeout: float = 8.0) -> None:
    """Espera hasta que un puerto TCP local acepte conexiones."""
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"Puerto TCP {port} no abrió en {timeout}s")


def esperar_puerto_o_tiempo(seg: float = 1.2) -> None:
    time.sleep(seg)


def test_1_funcional_completo() -> None:
    print("\n[TEST 1] Funcional completo (Consulta, Reserva, Modificación asientos/fecha, Cancelación + Notif)...")
    d = os.path.join(TMP_ROOT, "t1")
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d, exist_ok=True)
    env = env_suite(16100, d)
    p1 = int(env["SRB_PERSIST_PORT"])
    procs = []
    try:
        procs.append(lanzar("persistencia.py", ["--rol", "lider", "--reset", "--puerto", str(p1), "--db", env["SRB_DB_PATH"], "--metricas", os.path.join(d, "m_l.csv")], env, os.path.join(d, "lider.log")))
        esperar_puerto_tcp(p1)
        esperar_puerto_tcp(p1 + 1)
        esperar_puerto_tcp(p1 + 2)
        procs.append(lanzar("persistencia.py", ["--rol", "seguidor", "--id", "seg1", "--reset", "--puerto", str(p1 + 4), "--db", os.path.join(d, "seg1.db"), "--metricas", os.path.join(d, "m_s1.csv")], env, os.path.join(d, "seg1.log")))
        procs.append(lanzar("persistencia.py", ["--rol", "seguidor", "--id", "seg2", "--reset", "--puerto", str(p1 + 5), "--db", os.path.join(d, "seg2.db"), "--metricas", os.path.join(d, "m_s2.csv")], env, os.path.join(d, "seg2.log")))
        esperar_puerto_tcp(p1 + 4)
        esperar_puerto_tcp(p1 + 5)
        procs.append(lanzar("gestor_transacciones.py", ["--rol", "primario", "--metricas", os.path.join(d, "gt.csv")], env, os.path.join(d, "gt.log")))
        esperar_puerto_tcp(int(env["SRB_GT_FRONT_PORT"]))
        esperar_puerto_tcp(int(env["SRB_GT_BACK_PORT"]))
        esperar_puerto_tcp(int(env["SRB_GT_CANCEL_PUSH_PORT"]))
        procs.append(lanzar("rmc.py", ["--worker-id", "rmc-1", "--solo-primario"], env, os.path.join(d, "rmc1.log")))
        procs.append(lanzar("rmc.py", ["--worker-id", "rmc-2", "--solo-primario"], env, os.path.join(d, "rmc2.log")))
        procs.append(lanzar("cancelaciones.py", ["--worker-id", "c-1", "--solo-primario"], env, os.path.join(d, "cancel.log")))
        esperar_puerto_o_tiempo(0.8)

        res = subprocess.run(
            [sys.executable, os.path.join(BASE_DIR, "cliente_carga.py"), "--script", os.path.join(BASE_DIR, "casos_prueba", "caso_demo_profesor.json"), "--solo-primario"],
            env=env,
            cwd=BASE_DIR,
            capture_output=True,
            text=True,
        )
        if res.returncode != 0:
            logs_txt = ""
            for fn in sorted(os.listdir(d)):
                if fn.endswith(".log"):
                    with open(os.path.join(d, fn), encoding="utf-8", errors="replace") as lf:
                        logs_txt += f"\n--- {fn} ---\n{lf.read()}"
            raise AssertionError(
                f"cliente_carga rc={res.returncode}\nSTDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}\nLOGS:{logs_txt}"
            )
        assert "Notificaciones recibidas: 1" in res.stdout, res.stdout
        assert "SOLO_UNA_OCURRENCIA" in res.stdout, res.stdout

        time.sleep(0.4)
        import ver_bd

        c_l = ver_bd.abrir_ro(env["SRB_DB_PATH"])
        c_s1 = ver_bd.abrir_ro(os.path.join(d, "seg1.db"))
        c_s2 = ver_bd.abrir_ro(os.path.join(d, "seg2.db"))

        snap_l = ver_bd.snapshot_comparable(c_l)
        assert snap_l == ver_bd.snapshot_comparable(c_s1), "Seg1 difiere del Líder"
        assert snap_l == ver_bd.snapshot_comparable(c_s2), "Seg2 difiere del Líder"

        lib_01 = c_l.execute("SELECT asientos_libres FROM ocurrencias WHERE id_evento='E1' AND fecha='2026-11-01'").fetchone()[0]
        lib_15 = c_l.execute("SELECT asientos_libres FROM ocurrencias WHERE id_evento='E1' AND fecha='2026-11-15'").fetchone()[0]
        lib_21 = c_l.execute("SELECT asientos_libres FROM ocurrencias WHERE id_evento='E21' AND fecha='2026-11-07'").fetchone()[0]
        assert lib_01 == 100, f"E1 11-01 esperaba 100, obtuvo {lib_01}"
        assert lib_15 == 85, f"E1 11-15 esperaba 85, obtuvo {lib_15}"
        assert lib_21 == 100, f"E21 11-07 esperaba 100 tras cancelación, obtuvo {lib_21}"
        c_l.close(); c_s1.close(); c_s2.close()
        print("  -> OK (BD Líder y Seguidoras exactas, modificación/rechazo/notificación verificados)")
    finally:
        detener_todos(procs)


def test_2_concurrencia_e_idempotencia() -> None:
    print("\n[TEST 2] Concurrencia Strict 2PL (cero sobreventa), idempotencia y versión optimista...")
    d = os.path.join(TMP_ROOT, "t2")
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d, exist_ok=True)
    env = env_suite(16200, d, {"SRB_PERSIST_REPLICAS": ""})
    p1 = int(env["SRB_PERSIST_PORT"])
    procs = []
    try:
        procs.append(lanzar("persistencia.py", ["--rol", "lider", "--reset", "--modo-repl", "none", "--puerto", str(p1), "--db", env["SRB_DB_PATH"], "--metricas", os.path.join(d, "m_l.csv")], env, os.path.join(d, "lider.log")))
        esperar_puerto_tcp(p1)
        procs.append(lanzar("gestor_transacciones.py", ["--rol", "primario", "--metricas", os.path.join(d, "gt.csv")], env, os.path.join(d, "gt.log")))
        esperar_puerto_tcp(int(env["SRB_GT_FRONT_PORT"]))
        esperar_puerto_tcp(int(env["SRB_GT_BACK_PORT"]))
        for i in range(4):
            procs.append(lanzar("rmc.py", ["--worker-id", f"rmc-{i}", "--sin-replicas", "--solo-primario"], env, os.path.join(d, f"rmc{i}.log")))
        esperar_puerto_o_tiempo(0.8)

        ClienteSRB = cargar_cliente_con_env(env)
        barrera = threading.Barrier(10)
        resultados = []
        lock = threading.Lock()

        def worker(idx: int):
            cli = ClienteSRB(f"CONC-{idx}", solo_primario=True)
            try:
                barrera.wait()
                est, _, resp, _ = cli.pedir_sincrono(
                    "RESERVA",
                    {
                        "id_tx": str(uuid.uuid4()),
                        "id_cliente": f"CONC-{idx}",
                        "id_evento": "E5",
                        "fecha": "2026-11-05",
                        "num_asientos": 15,
                    },
                )
                with lock:
                    resultados.append((est, resp))
            finally:
                cli.cerrar()

        hilos = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
        for h in hilos:
            h.start()
        for h in hilos:
            h.join()

        exitos = [r for est, r in resultados if est == "EXITO"]
        rechazos = [r for est, r in resultados if est == "RECHAZO"]
        if len(exitos) != 6:
            for fn in sorted(os.listdir(d)):
                if fn.endswith(".log"):
                    print(f"--- {fn} ---\n" + open(os.path.join(d, fn), encoding="utf-8", errors="replace").read())
        assert len(exitos) == 6, f"Esperaba 6 éxitos (90 asientos), obtuvo {len(exitos)}: {resultados}"
        assert len(rechazos) == 4, f"Esperaba 4 rechazos, obtuvo {len(rechazos)}"

        # Idempotencia + Validación Optimista
        cli = ClienteSRB("IDEMP", solo_primario=True)
        try:
            tx_fija = "tx-idemp-unica-1"
            e1, _, r1, _ = cli.pedir_sincrono(
                "RESERVA",
                {"id_tx": tx_fija, "id_cliente": "IDEMP", "id_evento": "E6", "fecha": "2026-11-06", "num_asientos": 10},
            )
            e2, _, r2, _ = cli.pedir_sincrono(
                "RESERVA",
                {"id_tx": tx_fija, "id_cliente": "IDEMP", "id_evento": "E6", "fecha": "2026-11-06", "num_asientos": 10},
            )
            assert e1 == "EXITO" and e2 == "EXITO"
            assert r1["detalle"]["id_reserva"] == r2["detalle"]["id_reserva"]
            assert r2["detalle"].get("duplicada") is True

            rid = r1["detalle"]["id_reserva"]
            m1, _, _, _ = cli.pedir_sincrono(
                "MODIFICACION",
                {"id_tx": "tx-mod-1", "id_reserva": rid, "tipo": "ASIENTOS", "nuevo_num_asientos": 12, "version_esperada": 1},
            )
            m2, _, rm2, _ = cli.pedir_sincrono(
                "MODIFICACION",
                {"id_tx": "tx-mod-2", "id_reserva": rid, "tipo": "ASIENTOS", "nuevo_num_asientos": 20, "version_esperada": 1},
            )
            assert m1 == "EXITO" and m2 == "RECHAZO" and rm2["detalle"] == "CONFLICTO_VERSION"
        finally:
            cli.cerrar()

        conn = sqlite3.connect(env["SRB_DB_PATH"])
        lib_e5 = conn.execute("SELECT asientos_libres FROM ocurrencias WHERE id_evento='E5' AND fecha='2026-11-05'").fetchone()[0]
        lib_e6 = conn.execute("SELECT asientos_libres FROM ocurrencias WHERE id_evento='E6' AND fecha='2026-11-06'").fetchone()[0]
        conn.close()
        assert lib_e5 == 10, f"E5 esperaba 10 libres, obtuvo {lib_e5}"
        assert lib_e6 == 88, f"E6 esperaba 88 libres, obtuvo {lib_e6}"
        print("  -> OK (6 éxitos/4 rechazos exactos, cero sobreventa, idempotencia y versión OK)")
    finally:
        detener_todos(procs)


def test_3_replicacion_y_catchup() -> None:
    print("\n[TEST 3] Replicación semi-síncrona, Read-Your-Writes y Catch-Up tras caída de Seguidora...")
    d = os.path.join(TMP_ROOT, "t3")
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d, exist_ok=True)
    env = env_suite(16300, d)
    p1 = int(env["SRB_PERSIST_PORT"])
    procs = []
    try:
        p_lider = lanzar("persistencia.py", ["--rol", "lider", "--reset", "--puerto", str(p1), "--db", env["SRB_DB_PATH"], "--metricas", os.path.join(d, "m_l.csv")], env, os.path.join(d, "lider.log"))
        procs.append(p_lider)
        esperar_puerto_tcp(p1)
        esperar_puerto_tcp(p1 + 1)
        esperar_puerto_tcp(p1 + 2)
        p_seg1 = lanzar("persistencia.py", ["--rol", "seguidor", "--id", "seg1", "--reset", "--puerto", str(p1 + 4), "--db", os.path.join(d, "seg1.db"), "--metricas", os.path.join(d, "m_s1.csv")], env, os.path.join(d, "seg1.log"))
        p_seg2 = lanzar("persistencia.py", ["--rol", "seguidor", "--id", "seg2", "--reset", "--puerto", str(p1 + 5), "--db", os.path.join(d, "seg2.db"), "--metricas", os.path.join(d, "m_s2.csv")], env, os.path.join(d, "seg2.log"))
        procs.extend([p_seg1, p_seg2])
        esperar_puerto_tcp(p1 + 4)
        esperar_puerto_tcp(p1 + 5)
        procs.append(lanzar("gestor_transacciones.py", ["--rol", "primario", "--metricas", os.path.join(d, "gt.csv")], env, os.path.join(d, "gt.log")))
        esperar_puerto_tcp(int(env["SRB_GT_FRONT_PORT"]))
        esperar_puerto_tcp(int(env["SRB_GT_BACK_PORT"]))
        procs.append(lanzar("rmc.py", ["--worker-id", "rmc-1", "--solo-primario"], env, os.path.join(d, "rmc1.log")))
        esperar_puerto_o_tiempo(0.8)

        ClienteSRB = cargar_cliente_con_env(env)
        cli = ClienteSRB("REPL-C", solo_primario=True)
        try:
            # Reservar 25 en E10 y leer inmediatamente (Read-Your-Writes)
            est, _, r_res, _ = cli.pedir_sincrono(
                "RESERVA",
                {"id_tx": str(uuid.uuid4()), "id_cliente": "REPL-C", "id_evento": "E10", "fecha": "2026-11-10", "num_asientos": 25},
            )
            if est != "EXITO":
                for fn in sorted(os.listdir(d)):
                    if fn.endswith(".log"):
                        print(f"--- {fn} ---\n" + open(os.path.join(d, fn), encoding="utf-8", errors="replace").read())
            assert est == "EXITO", f"est={est} resp={r_res}"
            est_c, _, r_con, _ = cli.pedir_sincrono("CONSULTA", {"mes": "2026-11"})
            assert est_c == "EXITO"
            e10 = [o for o in r_con["detalle"] if o["id_evento"] == "E10" and o["fecha"] == "2026-11-10"][0]
            assert e10["asientos_libres"] == 75, f"Stale read detectado! libres={e10['asientos_libres']}"

            # Matar seg2, hacer 3 escrituras más y luego reiniciar seg2 sin --reset
            p_seg2.terminate()
            p_seg2.wait(timeout=2)
            for k in range(3):
                e_k, _, _, _ = cli.pedir_sincrono(
                    "RESERVA",
                    {"id_tx": str(uuid.uuid4()), "id_cliente": "REPL-C", "id_evento": f"E{11 + k}", "fecha": f"2026-11-{11 + k:02d}", "num_asientos": 5},
                )
                assert e_k == "EXITO"

            p_seg2 = lanzar("persistencia.py", ["--rol", "seguidor", "--id", "seg2", "--puerto", str(p1 + 5), "--db", os.path.join(d, "seg2.db"), "--metricas", os.path.join(d, "m_s2.csv")], env)
            procs.append(p_seg2)
            time.sleep(1.2)
        finally:
            cli.cerrar()

        import ver_bd

        cl = ver_bd.abrir_ro(env["SRB_DB_PATH"])
        cs1 = ver_bd.abrir_ro(os.path.join(d, "seg1.db"))
        cs2 = ver_bd.abrir_ro(os.path.join(d, "seg2.db"))
        assert ver_bd.snapshot_comparable(cl) == ver_bd.snapshot_comparable(cs1)
        assert ver_bd.snapshot_comparable(cl) == ver_bd.snapshot_comparable(cs2)
        assert ver_bd.resumen_bd(cs2)["seq"] == 4
        cl.close(); cs1.close(); cs2.close()
        print("  -> OK (Read-Your-Writes sin stale reads + Seg2 recuperó deltas faltantes por Catch-Up)")
    finally:
        detener_todos(procs)


def test_4_fallo_rmc_y_supervisor() -> None:
    print("\n[TEST 4] Tolerancia a fallos de RMC (kill en vuelo + reenvío GT + reemplazo por Supervisor)...")
    d = os.path.join(TMP_ROOT, "t4")
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d, exist_ok=True)
    env = env_suite(16400, d, {"SRB_PERSIST_REPLICAS": ""})
    p1 = int(env["SRB_PERSIST_PORT"])
    estado_sup = os.path.join(d, "sup.json")
    procs = []
    try:
        procs.append(lanzar("persistencia.py", ["--rol", "lider", "--reset", "--modo-repl", "none", "--puerto", str(p1), "--db", env["SRB_DB_PATH"], "--metricas", os.path.join(d, "m_l.csv")], env))
        esperar_puerto_tcp(p1)
        procs.append(lanzar("gestor_transacciones.py", ["--rol", "primario", "--metricas", os.path.join(d, "gt.csv")], env))
        esperar_puerto_tcp(int(env["SRB_GT_FRONT_PORT"]))
        esperar_puerto_tcp(int(env["SRB_GT_BACK_PORT"]))
        esperar_puerto_tcp(int(env["SRB_GT_CANCEL_PUSH_PORT"]))
        procs.append(lanzar("supervisor.py", ["--rmc", "3", "--cancel", "1", "--sin-replicas", "--solo-primario", "--estado-file", estado_sup], env))
        esperar_puerto_o_tiempo(1.0)

        with open(estado_sup, "r", encoding="utf-8") as f:
            pids_iniciales = json.load(f)
        pid_rmc1 = pids_iniciales["rmc-1"]

        ClienteSRB = cargar_cliente_con_env(env)
        exitos_asientos = [0]
        errores = [0]
        lock = threading.Lock()
        parar = threading.Event()

        def carga(cid: str):
            cli = ClienteSRB(cid, solo_primario=True, timeout_ms=2500)
            try:
                idx = 0
                while not parar.is_set():
                    idx += 1
                    est, _, _, _ = cli.pedir_sincrono(
                        "RESERVA",
                        {
                            "id_tx": f"{cid}-{idx}",
                            "id_cliente": cid,
                            "id_evento": f"E{(idx % 10) + 1}",
                            "fecha": f"2026-11-{(idx % 10) + 1:02d}",
                            "num_asientos": 2,
                        },
                    )
                    with lock:
                        if est == "EXITO":
                            exitos_asientos[0] += 2
                        elif est not in ("RECHAZO",):
                            errores[0] += 1
                    time.sleep(0.03)
            finally:
                cli.cerrar()

        h1 = threading.Thread(target=carga, args=("RMCT1",))
        h2 = threading.Thread(target=carga, args=("RMCT2",))
        h1.start(); h2.start()

        time.sleep(1.5)
        # Matar rmc-1 en caliente
        os.kill(pid_rmc1, signal.SIGTERM)
        time.sleep(2.5)
        parar.set()
        h1.join(); h2.join()

        with open(estado_sup, "r", encoding="utf-8") as f:
            pids_finales = json.load(f)
        assert pids_finales["rmc-1"] != pid_rmc1, "Supervisor no reemplazó rmc-1"
        assert errores[0] == 0, f"Hubo {errores[0]} errores/timeouts tras matar RMC"

        conn = sqlite3.connect(env["SRB_DB_PATH"])
        ocupados = conn.execute("SELECT SUM(asientos_totales - asientos_libres) FROM ocurrencias").fetchone()[0]
        conn.close()
        assert ocupados == exitos_asientos[0], f"Descuadre BD={ocupados} vs cliente={exitos_asientos[0]}"
        print(f"  -> OK (rmc-1 PID {pid_rmc1} -> {pids_finales['rmc-1']}, 0 errores, {ocupados} asientos exactos)")
    finally:
        detener_todos(procs)


def test_5_fallo_gt_primario() -> None:
    print("\n[TEST 5] Tolerancia a fallos de GT (kill de GT Primario -> promoción Backup + continuidad transparente)...")
    d = os.path.join(TMP_ROOT, "t5")
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d, exist_ok=True)
    env = env_suite(16500, d)
    p1 = int(env["SRB_PERSIST_PORT"])
    procs = []
    try:
        procs.append(lanzar("persistencia.py", ["--rol", "lider", "--reset", "--puerto", str(p1), "--db", env["SRB_DB_PATH"], "--metricas", os.path.join(d, "m_l.csv")], env))
        esperar_puerto_tcp(p1)
        esperar_puerto_tcp(p1 + 1)
        esperar_puerto_tcp(p1 + 2)
        procs.append(lanzar("persistencia.py", ["--rol", "seguidor", "--id", "seg1", "--reset", "--puerto", str(p1 + 4), "--db", os.path.join(d, "seg1.db"), "--metricas", os.path.join(d, "m_s1.csv")], env))
        esperar_puerto_tcp(p1 + 4)
        p_gt1 = lanzar("gestor_transacciones.py", ["--rol", "primario", "--metricas", os.path.join(d, "gt1.csv")], env)
        esperar_puerto_tcp(int(env["SRB_GT_FRONT_PORT"]))
        esperar_puerto_tcp(int(env["SRB_GT_BACK_PORT"]))
        esperar_puerto_tcp(int(env["SRB_GT_CANCEL_PUSH_PORT"]))
        p_gt2 = lanzar("gestor_transacciones.py", ["--rol", "backup", "--metricas", os.path.join(d, "gt2.csv")], env)
        esperar_puerto_tcp(int(env["SRB_GT_BACKUP_CANCEL_PUSH_PORT"]))
        procs.extend([p_gt1, p_gt2])
        procs.append(lanzar("supervisor.py", ["--rmc", "3", "--cancel", "2"], env))
        esperar_puerto_o_tiempo(1.0)

        ClienteSRB = cargar_cliente_con_env(env)
        asientos_ok = [0]
        errores = [0]
        reintentos_total = [0]
        notifs_esperadas = [0]
        notifs_recibidas = [0]
        lock = threading.Lock()
        parar = threading.Event()

        def carga(cid: str):
            cli = ClienteSRB(cid, timeout_ms=2000, max_reintentos=5)
            try:
                idx = 0
                ev_cancels = []
                while not parar.is_set():
                    idx += 1
                    est, _, resp, reint = cli.pedir_sincrono(
                        "RESERVA",
                        {
                            "id_tx": f"{cid}-res-{idx}",
                            "id_cliente": cid,
                            "id_evento": f"E{(idx % 10) + 1}",
                            "fecha": f"2026-11-{(idx % 10) + 1:02d}",
                            "num_asientos": 2,
                        },
                    )
                    with lock:
                        reintentos_total[0] += reint
                        if est == "EXITO":
                            asientos_ok[0] += 2
                        else:
                            errores[0] += 1

                    # Enviar una cancelación después de que caiga el GT Primario también
                    if est == "EXITO" and idx % 5 == 0:
                        rid = resp["detalle"]["id_reserva"]
                        est_c, _, _, re_c, ev = cli.pedir_cancelacion(
                            {"id_tx": f"{cid}-can-{idx}", "id_cliente": cid, "id_reserva": rid}
                        )
                        with lock:
                            reintentos_total[0] += re_c
                            if est_c == "ACEPTADA":
                                asientos_ok[0] -= 2
                                notifs_esperadas[0] += 1
                                ev_cancels.append(ev)
                            else:
                                errores[0] += 1
                    time.sleep(0.08)

                for ev in ev_cancels:
                    if ev.wait(timeout=4.0):
                        with lock:
                            notifs_recibidas[0] += 1
            finally:
                cli.cerrar()

        h1 = threading.Thread(target=carga, args=("GTT1",))
        h2 = threading.Thread(target=carga, args=("GTT2",))
        h1.start(); h2.start()

        time.sleep(2.0)
        # Matar GT Primario en medio del tráfico
        p_gt1.kill()
        p_gt1.wait(timeout=2)
        time.sleep(4.5)
        parar.set()
        h1.join(); h2.join()

        assert errores[0] == 0, f"Hubo {errores[0]} operaciones perdidas tras failover de GT"
        assert reintentos_total[0] > 0, "No se detectaron reintentos de failover hacia GT Backup"
        assert notifs_recibidas[0] == notifs_esperadas[0], (
            f"Notificaciones {notifs_recibidas[0]} != {notifs_esperadas[0]}"
        )

        conn = sqlite3.connect(env["SRB_DB_PATH"])
        ocupados = conn.execute("SELECT SUM(asientos_totales - asientos_libres) FROM ocurrencias").fetchone()[0]
        conn.close()
        assert ocupados == asientos_ok[0], f"Descuadre BD={ocupados} vs cliente={asientos_ok[0]}"
        print(
            f"  -> OK (GT Backup asumió operación, reintentos={reintentos_total[0]}, "
            f"0 pérdidas, notifs={notifs_recibidas[0]}/{notifs_esperadas[0]}, asientos={ocupados})"
        )
    finally:
        detener_todos(procs)


def main() -> None:
    os.makedirs(TMP_ROOT, exist_ok=True)
    try:
        test_1_funcional_completo()
        test_2_concurrencia_e_idempotencia()
        test_3_replicacion_y_catchup()
        test_4_fallo_rmc_y_supervisor()
        test_5_fallo_gt_primario()
        print("\n=== TODAS LAS 5 PRUEBAS PASARON EXITOSAMENTE ===")
    finally:
        shutil.rmtree(TMP_ROOT, ignore_errors=True)


if __name__ == "__main__":
    main()
