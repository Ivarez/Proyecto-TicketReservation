"""Servicio de Persistencia del SRB (Líder Único con Réplicas Seguidoras).

Responsabilidades y patrones implementados:
- **Líder Único (Single-Leader)**: todas las escrituras (RESERVA, MODIFICACION, CANCELACION)
  se serializan en el nodo Líder dentro de transacciones ACID SQLite con modo WAL y
  ``BEGIN IMMEDIATE`` (bloqueo estricto equivalente a Strict 2PL: evita *Lost Update*,
  *Dirty Read*, abortos en cascada y *Deadlocks*).
- **Idempotencia global (Exactly-Once Effect)**: tabla ``tx_procesadas`` dentro de la misma
  transacción ACID evita duplicar reservas, modificaciones o cancelaciones ante reintentos
  de red o *failover* del Gestor de Transacciones o RMC.
- **Validación Optimista**: columna ``version`` en ``reservas`` detecta modificaciones
  concurrentes sobre la misma reserva cuando se especifica ``version_esperada``.
- **Replicación PUSH de Estado Idempotente + PULL Catch-Up**:
  - Modos soportados: ``semi-sincrono`` (por defecto: espera ACK de al menos 1 seguidora
    hasta ``REPL_ACK_TIMEOUT_MS``), ``sincrono``, ``asincrono`` y ``none``.
  - Propaga snapshots de filas modificadas con número de secuencia lógico ``seq`` (LSN)
    en lugar de operaciones incrementales, evitando problemas de orden, no determinismo e
    idempotencia de la replicación activa.
  - Canal ``catch-up`` bajo demanda (REQ/REP) para sincronizar seguidoras recién iniciadas
    o recuperadas tras una caída.
- **Consistencia Centrada en el Cliente**: las consultas aceptan ``min_seq``; las réplicas
  seguidoras esperan brevemente o ejecutan *catch-up* hasta alcanzar ``min_seq`` para
  garantizar *Read-Your-Writes* y *Monotonic Reads*.
"""
import argparse
import csv
import json
import os
import sqlite3
import threading
import time
import zmq

from config import (
    ASIENTOS_POR_EVENTO,
    DB_PATH,
    MES_BASE,
    MODO_REPLICACION,
    PERSIST_CATCHUP_PORT,
    PERSIST_HB_PORT,
    PERSIST_HOST,
    PERSIST_PORT,
    PERSIST_REPL_PORT,
    REPL_ACK_TIMEOUT_MS,
)

ESQUEMA = """
CREATE TABLE IF NOT EXISTS eventos(
  id_evento TEXT PRIMARY KEY,
  nombre TEXT NOT NULL,
  ocurrencias_mes INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS ocurrencias(
  id_ocurrencia INTEGER PRIMARY KEY AUTOINCREMENT,
  id_evento TEXT NOT NULL REFERENCES eventos(id_evento),
  fecha TEXT NOT NULL,
  asientos_totales INTEGER NOT NULL CHECK(asientos_totales > 0),
  asientos_libres INTEGER NOT NULL CHECK(asientos_libres >= 0 AND asientos_libres <= asientos_totales),
  UNIQUE(id_evento, fecha)
);
CREATE TABLE IF NOT EXISTS reservas(
  id_reserva INTEGER PRIMARY KEY AUTOINCREMENT,
  id_tx TEXT UNIQUE,
  id_cliente TEXT NOT NULL,
  id_ocurrencia INTEGER NOT NULL REFERENCES ocurrencias(id_ocurrencia),
  num_asientos INTEGER NOT NULL CHECK(num_asientos > 0),
  estado TEXT NOT NULL DEFAULT 'ACTIVA',
  version INTEGER NOT NULL DEFAULT 1,
  creada_en REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS tx_procesadas(
  id_tx TEXT PRIMARY KEY,
  op TEXT NOT NULL,
  respuesta_json TEXT NOT NULL,
  seq INTEGER NOT NULL,
  creada_en REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS log_replicacion(
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  op TEXT NOT NULL,
  cambios_json TEXT NOT NULL,
  ts REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS meta_estado(
  clave TEXT PRIMARY KEY,
  valor TEXT NOT NULL
);
"""


def abrir_conexion(db_path: str) -> sqlite3.Connection:
    """Abre una conexión SQLite endurecida con WAL, claves foráneas y sync NORMAL."""
    carpeta = os.path.dirname(os.path.abspath(db_path))
    if carpeta:
        os.makedirs(carpeta, exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.isolation_level = None  # Control manual explícito de BEGIN IMMEDIATE / COMMIT
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.executescript(ESQUEMA)
    return conn


def borrar_archivos_bd(db_path: str) -> None:
    """Elimina la BD y sus archivos auxiliares WAL/SHM para un reinicio limpio."""
    for sufijo in ("", "-wal", "-shm"):
        ruta = db_path + sufijo
        if os.path.exists(ruta):
            os.remove(ruta)


def obtener_seq_bd(conn: sqlite3.Connection) -> int:
    """Retorna el último LSN (``seq``) persistido en la BD."""
    fila = conn.execute("SELECT valor FROM meta_estado WHERE clave='ultimo_seq'").fetchone()
    if fila:
        return int(fila["valor"])
    fila = conn.execute("SELECT COALESCE(MAX(seq), 0) AS s FROM log_replicacion").fetchone()
    return int(fila["s"]) if fila else 0


def sembrar(
    conn: sqlite3.Connection,
    mes: str = MES_BASE,
    num_eventos: int = 50,
    num_dobles: int = 20,
    asientos: int = ASIENTOS_POR_EVENTO,
    seed_file: str | None = None,
) -> None:
    """Carga los eventos iniciales en el Líder si la tabla está vacía."""
    if conn.execute("SELECT COUNT(*) FROM eventos").fetchone()[0]:
        return

    conn.execute("BEGIN IMMEDIATE")
    try:
        if seed_file:
            with open(seed_file, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            mes = cfg.get("mes", mes)
            if "eventos_lista" in cfg:
                for ev in cfg["eventos_lista"]:
                    fechas = ev["fechas"]
                    conn.execute(
                        "INSERT INTO eventos(id_evento, nombre, ocurrencias_mes) VALUES (?,?,?)",
                        (ev["id_evento"], ev.get("nombre", ev["id_evento"]), len(fechas)),
                    )
                    for oc in fechas:
                        tot = int(oc.get("asientos", asientos))
                        lib = int(oc.get("asientos_libres", tot))
                        conn.execute(
                            "INSERT INTO ocurrencias(id_evento,fecha,asientos_totales,asientos_libres)"
                            " VALUES (?,?,?,?)",
                            (ev["id_evento"], oc["fecha"], tot, lib),
                        )
                conn.execute("INSERT OR REPLACE INTO meta_estado VALUES ('ultimo_seq', '0')")
                conn.execute("COMMIT")
                return
            num_eventos = int(cfg.get("eventos", num_eventos))
            num_dobles = int(cfg.get("dobles", num_dobles))
            asientos = int(cfg.get("asientos", asientos))

        num_dobles = min(num_dobles, num_eventos)
        for i in range(1, num_eventos + 1):
            eid = f"E{i}"
            n = 2 if i <= num_dobles else 1
            conn.execute(
                "INSERT INTO eventos(id_evento, nombre, ocurrencias_mes) VALUES (?,?,?)",
                (eid, f"Evento {i}", n),
            )
            for k in range(n):
                dia = ((i - 1) % 14) + 1 + 14 * k
                conn.execute(
                    "INSERT INTO ocurrencias(id_evento,fecha,asientos_totales,asientos_libres)"
                    " VALUES (?,?,?,?)",
                    (eid, f"{mes}-{dia:02d}", asientos, asientos),
                )
        conn.execute("INSERT OR REPLACE INTO meta_estado VALUES ('ultimo_seq', '0')")
        conn.execute("COMMIT")
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise


def resumen(conn: sqlite3.Connection) -> str:
    """Resumen compacto del estado actual de asientos, reservas y ``seq``."""
    row = conn.execute(
        "SELECT COALESCE(SUM(asientos_totales),0), COALESCE(SUM(asientos_libres),0) FROM ocurrencias"
    ).fetchone()
    t, l = int(row[0]), int(row[1])
    act = conn.execute("SELECT COUNT(*) FROM reservas WHERE estado='ACTIVA'").fetchone()[0]
    can = conn.execute("SELECT COUNT(*) FROM reservas WHERE estado='CANCELADA'").fetchone()[0]
    seq = obtener_seq_bd(conn)
    return (
        f"seq={seq} totales={t} libres={l} ocupados={t - l} "
        f"reservas_activas={act} canceladas={can}"
    )


def _registrar_replicacion(
    conn: sqlite3.Connection,
    op: str,
    ocurrencia_ids: list[int],
    reserva_ids: list[int],
    tx_row: tuple[str, str, float] | None = None,
) -> tuple[int, dict]:
    """Serializa el snapshot de filas modificadas dentro de la transacción activa."""
    oc_rows = []
    for oid in sorted(set(ocurrencia_ids)):
        r = conn.execute("SELECT * FROM ocurrencias WHERE id_ocurrencia=?", (oid,)).fetchone()
        if r:
            oc_rows.append(dict(r))

    res_rows = []
    for rid in sorted(set(reserva_ids)):
        r = conn.execute("SELECT * FROM reservas WHERE id_reserva=?", (rid,)).fetchone()
        if r:
            res_rows.append(dict(r))

    cambios = {"ocurrencias": oc_rows, "reservas": res_rows}
    if tx_row is not None:
        cambios["tx"] = {
            "id_tx": tx_row[0],
            "op": tx_row[1],
            "creada_en": tx_row[2],
        }

    ts = time.time()
    cur = conn.execute(
        "INSERT INTO log_replicacion(op, cambios_json, ts) VALUES (?,?,?)",
        (op, json.dumps(cambios), ts),
    )
    seq = int(cur.lastrowid)
    conn.execute("INSERT OR REPLACE INTO meta_estado VALUES ('ultimo_seq', ?)", (str(seq),))
    return seq, cambios


def _guardar_tx(
    conn: sqlite3.Connection, id_tx: str, op: str, resp: dict, seq: int, ahora: float
) -> None:
    conn.execute(
        "INSERT INTO tx_procesadas(id_tx, op, respuesta_json, seq, creada_en) VALUES (?,?,?,?,?)",
        (id_tx, op, json.dumps(resp), seq, ahora),
    )


def _buscar_tx_previa(conn: sqlite3.Connection, id_tx: str) -> dict | None:
    prev = conn.execute(
        "SELECT respuesta_json FROM tx_procesadas WHERE id_tx=?", (id_tx,)
    ).fetchone()
    if not prev:
        return None
    resp = json.loads(prev["respuesta_json"])
    if isinstance(resp.get("data"), dict):
        resp["data"]["duplicada"] = True
    resp["duplicada"] = True
    return resp


class ReplicadorLider:
    """Canal PUSH/ACK (`ROUTER`) y Catch-Up (`REP`) del nodo Líder hacia Seguidoras."""

    def __init__(
        self,
        db_path: str,
        server_id: str,
        modo_repl: str,
        repl_port: int,
        catchup_port: int,
        hb_port: int,
        ack_timeout_ms: int,
    ):
        self.db_path = db_path
        self.server_id = server_id
        self.modo_repl = modo_repl
        self.repl_port = repl_port
        self.catchup_port = catchup_port
        self.hb_port = hb_port
        self.ack_timeout_ms = ack_timeout_ms
        self.ctx = zmq.Context.instance()
        self.inproc_addr = f"inproc://repl_cmd_{server_id}_{id(self)}"
        self.cmd_push = self.ctx.socket(zmq.PUSH)
        self.cmd_push.setsockopt(zmq.LINGER, 0)
        self.seguidoras: dict[bytes, dict] = {}
        self.ack_lock = threading.Lock()
        self.pending_acks: dict[int, dict] = {}
        self._stop = threading.Event()
        self._seq_actual = 0

    def iniciar(self, seq_inicial: int) -> None:
        self._seq_actual = seq_inicial
        t_repl = threading.Thread(target=self._loop_replicacion, daemon=True)
        t_catch = threading.Thread(target=self._loop_catchup, daemon=True)
        t_hb = threading.Thread(target=self._loop_heartbeat, daemon=True)
        t_repl.start()
        t_catch.start()
        t_hb.start()
        # Conecta el extremo PUSH después de un instante para que PULL ya esté bindeado
        for _ in range(20):
            try:
                self.cmd_push.connect(self.inproc_addr)
                break
            except zmq.ZMQError:
                time.sleep(0.02)

    def detener(self) -> None:
        self._stop.set()
        try:
            self.cmd_push.close()
        except Exception:
            pass

    def _loop_replicacion(self) -> None:
        router = self.ctx.socket(zmq.ROUTER)
        router.setsockopt(zmq.LINGER, 0)
        router.bind(f"tcp://*:{self.repl_port}")

        cmd_pull = self.ctx.socket(zmq.PULL)
        cmd_pull.setsockopt(zmq.LINGER, 0)
        cmd_pull.bind(self.inproc_addr)

        poller = zmq.Poller()
        poller.register(router, zmq.POLLIN)
        poller.register(cmd_pull, zmq.POLLIN)

        try:
            while not self._stop.is_set():
                evts = dict(poller.poll(200))
                ahora = time.time()

                if router in evts:
                    frames = router.recv_multipart()
                    if len(frames) >= 2:
                        ident, raw = frames[0], frames[-1]
                        try:
                            msg = json.loads(raw)
                        except ValueError:
                            msg = {}
                        tipo = msg.get("tipo")
                        self.seguidoras[ident] = {
                            "id": msg.get("id", "?"),
                            "last_seen": ahora,
                        }
                        if tipo == "REGISTER":
                            print(
                                f"[PERSISTENCIA-LIDER] Seguidora registrada: {msg.get('id')} "
                                f"(seq={msg.get('seq')})",
                                flush=True,
                            )
                        elif tipo == "ACK":
                            seq = msg.get("seq")
                            with self.ack_lock:
                                info = self.pending_acks.get(seq)
                                if info is not None:
                                    info["acks"].add(ident)
                                    if len(info["acks"]) >= info["req"]:
                                        info["event"].set()

                if cmd_pull in evts:
                    raw_cmd = cmd_pull.recv()
                    # Limpia seguidoras que no han enviado HB en > 5 s
                    muertas = [
                        k for k, v in self.seguidoras.items() if ahora - v["last_seen"] > 5.0
                    ]
                    for k in muertas:
                        self.seguidoras.pop(k, None)

                    cmd = json.loads(raw_cmd)
                    seq = cmd["seq"]
                    self._seq_actual = seq
                    if not self.seguidoras:
                        with self.ack_lock:
                            info = self.pending_acks.get(seq)
                            if info is not None:
                                info["event"].set()
                        continue

                    payload = json.dumps(
                        {"tipo": "REPL", "seq": seq, "op": cmd["op"], "cambios": cmd["cambios"]}
                    ).encode()
                    for ident in list(self.seguidoras.keys()):
                        try:
                            router.send_multipart([ident, b"", payload], flags=zmq.NOBLOCK)
                        except zmq.Again:
                            pass
        finally:
            router.close()
            cmd_pull.close()

    def _loop_catchup(self) -> None:
        conn = abrir_conexion(self.db_path)
        sock = self.ctx.socket(zmq.REP)
        sock.setsockopt(zmq.LINGER, 0)
        sock.setsockopt(zmq.RCVTIMEO, 500)
        sock.bind(f"tcp://*:{self.catchup_port}")
        try:
            while not self._stop.is_set():
                try:
                    msg = sock.recv_json()
                except zmq.Again:
                    continue
                except zmq.ZMQError:
                    break
                desde = int(msg.get("desde_seq", 0))
                try:
                    conn.execute("BEGIN")
                    ultimo_seq = obtener_seq_bd(conn)
                    if desde <= 0:
                        resp = {
                            "ok": True,
                            "modo": "snapshot",
                            "ultimo_seq": ultimo_seq,
                            "eventos": [
                                dict(r)
                                for r in conn.execute("SELECT * FROM eventos").fetchall()
                            ],
                            "ocurrencias": [
                                dict(r)
                                for r in conn.execute("SELECT * FROM ocurrencias").fetchall()
                            ],
                            "reservas": [
                                dict(r)
                                for r in conn.execute("SELECT * FROM reservas").fetchall()
                            ],
                            "tx_procesadas": [
                                dict(r)
                                for r in conn.execute(
                                    "SELECT id_tx, op, respuesta_json, seq, creada_en FROM tx_procesadas"
                                ).fetchall()
                            ],
                        }
                    else:
                        deltas = [
                            {
                                "seq": r["seq"],
                                "op": r["op"],
                                "cambios": json.loads(r["cambios_json"]),
                            }
                            for r in conn.execute(
                                "SELECT seq, op, cambios_json FROM log_replicacion "
                                "WHERE seq > ? ORDER BY seq",
                                (desde,),
                            ).fetchall()
                        ]
                        resp = {
                            "ok": True,
                            "modo": "deltas",
                            "ultimo_seq": ultimo_seq,
                            "deltas": deltas,
                        }
                    conn.execute("COMMIT")
                except Exception as exc:
                    if conn.in_transaction:
                        conn.execute("ROLLBACK")
                    resp = {"ok": False, "error": str(exc)}
                sock.send_json(resp)
        finally:
            sock.close()
            conn.close()

    def _loop_heartbeat(self) -> None:
        pub = self.ctx.socket(zmq.PUB)
        pub.setsockopt(zmq.LINGER, 0)
        pub.bind(f"tcp://*:{self.hb_port}")
        try:
            while not self._stop.is_set():
                pub.send_json(
                    {
                        "rol": "lider",
                        "id": self.server_id,
                        "seq": self._seq_actual,
                        "ts": time.time(),
                    }
                )
                self._stop.wait(0.5)
        finally:
            pub.close()

    def propagar(self, seq: int, op: str, cambios: dict) -> int:
        """Envía el cambio a las seguidoras y espera ACK según el modo configurado."""
        if self.modo_repl == "none":
            self._seq_actual = seq
            return 0

        activos = len(self.seguidoras)
        ev = None
        if self.modo_repl in ("semi-sincrono", "sincrono") and activos > 0:
            req = 1 if self.modo_repl == "semi-sincrono" else activos
            ev = threading.Event()
            with self.ack_lock:
                self.pending_acks[seq] = {"event": ev, "req": req, "acks": set()}

        self.cmd_push.send_json({"seq": seq, "op": op, "cambios": cambios})

        acks_recibidos = 0
        if ev is not None:
            ev.wait(timeout=self.ack_timeout_ms / 1000.0)
            with self.ack_lock:
                info = self.pending_acks.pop(seq, None)
                if info:
                    acks_recibidos = len(info["acks"])
        return acks_recibidos


class ReceptorSeguidor:
    """Receptor PUSH y cliente de Catch-Up PULL para réplicas Seguidoras (solo lectura)."""

    def __init__(
        self,
        db_path: str,
        server_id: str,
        lider_host: str,
        repl_port: int,
        catchup_port: int,
    ):
        self.db_path = db_path
        self.server_id = server_id
        self.lider_host = lider_host
        self.repl_port = repl_port
        self.catchup_port = catchup_port
        self.ctx = zmq.Context.instance()
        self.repl_lock = threading.Lock()
        self.seq_cond = threading.Condition()
        self.mi_seq = 0
        self._stop = threading.Event()

    def get_seq(self) -> int:
        with self.seq_cond:
            return self.mi_seq

    def _aplicar_snapshot(self, conn: sqlite3.Connection, snap: dict) -> int:
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute("DELETE FROM tx_procesadas")
            conn.execute("DELETE FROM reservas")
            conn.execute("DELETE FROM ocurrencias")
            conn.execute("DELETE FROM eventos")
            conn.execute("DELETE FROM log_replicacion")

            for e in snap["eventos"]:
                conn.execute(
                    "INSERT INTO eventos(id_evento, nombre, ocurrencias_mes) VALUES (?,?,?)",
                    (e["id_evento"], e["nombre"], e["ocurrencias_mes"]),
                )
            for o in snap["ocurrencias"]:
                conn.execute(
                    "INSERT INTO ocurrencias(id_ocurrencia, id_evento, fecha, asientos_totales, asientos_libres)"
                    " VALUES (?,?,?,?,?)",
                    (
                        o["id_ocurrencia"],
                        o["id_evento"],
                        o["fecha"],
                        o["asientos_totales"],
                        o["asientos_libres"],
                    ),
                )
            for r in snap["reservas"]:
                conn.execute(
                    "INSERT INTO reservas(id_reserva, id_tx, id_cliente, id_ocurrencia, num_asientos, estado, version, creada_en)"
                    " VALUES (?,?,?,?,?,?,?,?)",
                    (
                        r["id_reserva"],
                        r["id_tx"],
                        r["id_cliente"],
                        r["id_ocurrencia"],
                        r["num_asientos"],
                        r["estado"],
                        r["version"],
                        r["creada_en"],
                    ),
                )
            for tx in snap.get("tx_procesadas", []):
                conn.execute(
                    "INSERT OR REPLACE INTO tx_procesadas(id_tx, op, respuesta_json, seq, creada_en)"
                    " VALUES (?,?,?,?,?)",
                    (tx["id_tx"], tx["op"], tx["respuesta_json"], tx["seq"], tx["creada_en"]),
                )
            ultimo_seq = int(snap["ultimo_seq"])
            conn.execute(
                "INSERT OR REPLACE INTO meta_estado VALUES ('ultimo_seq', ?)",
                (str(ultimo_seq),),
            )
            conn.execute("COMMIT")
            return ultimo_seq
        except Exception:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise

    def _aplicar_cambios_en_tx(
        self, conn: sqlite3.Connection, seq: int, op: str, cambios: dict
    ) -> None:
        """Aplica de forma idempotente el snapshot parcial de un ``seq``."""
        for o in cambios.get("ocurrencias", []):
            conn.execute(
                "INSERT OR REPLACE INTO ocurrencias(id_ocurrencia, id_evento, fecha, asientos_totales, asientos_libres)"
                " VALUES (?,?,?,?,?)",
                (
                    o["id_ocurrencia"],
                    o["id_evento"],
                    o["fecha"],
                    o["asientos_totales"],
                    o["asientos_libres"],
                ),
            )
        for r in cambios.get("reservas", []):
            conn.execute(
                "INSERT OR REPLACE INTO reservas(id_reserva, id_tx, id_cliente, id_ocurrencia, num_asientos, estado, version, creada_en)"
                " VALUES (?,?,?,?,?,?,?,?)",
                (
                    r["id_reserva"],
                    r["id_tx"],
                    r["id_cliente"],
                    r["id_ocurrencia"],
                    r["num_asientos"],
                    r["estado"],
                    r["version"],
                    r["creada_en"],
                ),
            )
        conn.execute(
            "INSERT OR IGNORE INTO log_replicacion(seq, op, cambios_json, ts) VALUES (?,?,?,?)",
            (seq, op, json.dumps(cambios), time.time()),
        )
        conn.execute("INSERT OR REPLACE INTO meta_estado VALUES ('ultimo_seq', ?)", (str(seq),))

    def catch_up(self, conn: sqlite3.Connection | None = None) -> bool:
        """Sincroniza desde el Líder vía REQ/REP (snapshot inicial o deltas faltantes)."""
        propio = conn is None
        if propio:
            conn = abrir_conexion(self.db_path)
        sock = self.ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.LINGER, 0)
        sock.setsockopt(zmq.RCVTIMEO, 2000)
        sock.setsockopt(zmq.SNDTIMEO, 2000)
        sock.connect(f"tcp://{self.lider_host}:{self.catchup_port}")
        try:
            with self.repl_lock:
                desde = obtener_seq_bd(conn)
                if conn.execute("SELECT COUNT(*) FROM eventos").fetchone()[0] == 0:
                    desde = 0
                sock.send_json({"desde_seq": desde})
                resp = sock.recv_json()
                if not resp.get("ok"):
                    return False
                if resp["modo"] == "snapshot":
                    nuevo_seq = self._aplicar_snapshot(conn, resp)
                else:
                    deltas = resp.get("deltas", [])
                    nuevo_seq = desde
                    if deltas:
                        conn.execute("BEGIN IMMEDIATE")
                        try:
                            for d in deltas:
                                if int(d["seq"]) > nuevo_seq:
                                    self._aplicar_cambios_en_tx(
                                        conn, int(d["seq"]), d["op"], d["cambios"]
                                    )
                                    nuevo_seq = int(d["seq"])
                            conn.execute("COMMIT")
                        except Exception:
                            if conn.in_transaction:
                                conn.execute("ROLLBACK")
                            raise
                with self.seq_cond:
                    self.mi_seq = max(self.mi_seq, nuevo_seq)
                    self.seq_cond.notify_all()
                return True
        except zmq.Again:
            return False
        finally:
            sock.close()
            if propio:
                conn.close()

    def esperar_seq(self, min_seq: int, timeout_s: float = 0.15) -> int:
        """Garantiza Read-Your-Writes / Monotonic Reads esperando o ejecutando catch-up."""
        if min_seq <= 0 or self.get_seq() >= min_seq:
            return self.get_seq()
        deadline = time.perf_counter() + timeout_s
        with self.seq_cond:
            while self.mi_seq < min_seq:
                rem = deadline - time.perf_counter()
                if rem <= 0:
                    break
                self.seq_cond.wait(timeout=rem)
        if self.get_seq() < min_seq:
            self.catch_up()
        return self.get_seq()

    def iniciar(self) -> None:
        # Sincronización inicial (con reintento breve si el Líder está arrancando)
        for _ in range(15):
            if self.catch_up():
                break
            time.sleep(0.2)
        t = threading.Thread(target=self._loop_receptor, daemon=True)
        t.start()

    def detener(self) -> None:
        self._stop.set()

    def _loop_receptor(self) -> None:
        conn = abrir_conexion(self.db_path)
        dealer = self.ctx.socket(zmq.DEALER)
        dealer.setsockopt(zmq.LINGER, 0)
        dealer.setsockopt(zmq.RECONNECT_IVL, 100)
        dealer.setsockopt(zmq.RECONNECT_IVL_MAX, 500)
        dealer.setsockopt(zmq.IDENTITY, f"{self.server_id}-{os.getpid()}".encode())
        dealer.connect(f"tcp://{self.lider_host}:{self.repl_port}")

        poller = zmq.Poller()
        poller.register(dealer, zmq.POLLIN)

        def enviar(payload: dict) -> None:
            dealer.send_multipart([b"", json.dumps(payload).encode()])

        enviar({"tipo": "REGISTER", "id": self.server_id, "seq": self.get_seq()})
        ultimo_hb = time.time()

        try:
            while not self._stop.is_set():
                evts = dict(poller.poll(200))
                ahora = time.time()
                if ahora - ultimo_hb >= 1.0:
                    enviar({"tipo": "HB", "id": self.server_id})
                    ultimo_hb = ahora

                if dealer not in evts:
                    continue
                frames = dealer.recv_multipart()
                raw = frames[-1]
                msg = json.loads(raw)
                if msg.get("tipo") != "REPL":
                    continue

                seq = int(msg["seq"])
                op = msg["op"]
                cambios = msg["cambios"]

                with self.repl_lock:
                    actual = obtener_seq_bd(conn)
                    if seq <= actual:
                        # Cambio ya aplicado: reconfirmar idempotente
                        enviar({"tipo": "ACK", "id": self.server_id, "seq": seq})
                        continue
                    if seq > actual + 1:
                        # Brecha detectada (ej. caída temporal): recuperar deltas por PULL
                        pass
                if seq > actual + 1:
                    self.catch_up(conn)
                else:
                    with self.repl_lock:
                        actual = obtener_seq_bd(conn)
                        if seq == actual + 1:
                            conn.execute("BEGIN IMMEDIATE")
                            try:
                                self._aplicar_cambios_en_tx(conn, seq, op, cambios)
                                conn.execute("COMMIT")
                            except Exception:
                                if conn.in_transaction:
                                    conn.execute("ROLLBACK")
                                raise
                            with self.seq_cond:
                                self.mi_seq = seq
                                self.seq_cond.notify_all()
                enviar({"tipo": "ACK", "id": self.server_id, "seq": seq})
        finally:
            dealer.close()
            conn.close()


# ---------------------------------------------------------------------------
# Operaciones CRUD (Lectura y Escritura)
# ---------------------------------------------------------------------------

def consultar_eventos(
    conn: sqlite3.Connection, d: dict, server_id: str, receptor: ReceptorSeguidor | None
) -> dict:
    min_seq = int(d.get("min_seq", 0) or 0)
    if receptor is not None and min_seq > 0:
        receptor.esperar_seq(min_seq)

    mes = str(d.get("mes", MES_BASE))
    filas = conn.execute(
        """SELECT o.id_ocurrencia, o.id_evento, e.nombre, e.ocurrencias_mes,
                  o.fecha, o.asientos_totales, o.asientos_libres,
                  (o.asientos_totales - o.asientos_libres) AS asientos_ocupados
           FROM ocurrencias o JOIN eventos e ON e.id_evento = o.id_evento
           WHERE o.fecha LIKE ?
           ORDER BY o.fecha, CAST(SUBSTR(o.id_evento, 2) AS INT), o.id_evento""",
        (mes + "-%",),
    ).fetchall()
    seq = receptor.get_seq() if receptor is not None else obtener_seq_bd(conn)
    return {"ok": True, "data": [dict(f) for f in filas], "seq": seq, "origen": server_id}


def disponibilidad(
    conn: sqlite3.Connection, d: dict, server_id: str, receptor: ReceptorSeguidor | None
) -> dict:
    id_tx = d.get("id_tx")
    if id_tx:
        previa = _buscar_tx_previa(conn, str(id_tx))
        if previa:
            return {"ok": True, "tx_previa": previa, "seq": previa.get("seq", 0), "origen": server_id}
    f = conn.execute(
        """SELECT o.id_ocurrencia, o.id_evento, o.fecha, o.asientos_totales,
                  o.asientos_libres, e.ocurrencias_mes
           FROM ocurrencias o JOIN eventos e ON e.id_evento = o.id_evento
           WHERE o.id_evento=? AND o.fecha=?""",
        (d["id_evento"], d["fecha"]),
    ).fetchone()
    if not f:
        return {"ok": False, "error": "EVENTO_NO_EXISTE"}
    seq = receptor.get_seq() if receptor is not None else obtener_seq_bd(conn)
    return {"ok": True, "data": dict(f), "seq": seq, "origen": server_id}


def obtener_reserva(
    conn: sqlite3.Connection, d: dict, server_id: str, receptor: ReceptorSeguidor | None
) -> dict:
    id_tx = d.get("id_tx")
    if id_tx:
        previa = _buscar_tx_previa(conn, str(id_tx))
        if previa:
            return {"ok": True, "tx_previa": previa, "seq": previa.get("seq", 0), "origen": server_id}
    f = conn.execute(
        """SELECT r.id_reserva, r.id_tx, r.id_cliente, r.id_ocurrencia,
                  r.num_asientos, r.estado, r.version, r.creada_en,
                  o.id_evento, o.fecha, o.asientos_totales, o.asientos_libres,
                  e.ocurrencias_mes
           FROM reservas r
           JOIN ocurrencias o ON o.id_ocurrencia = r.id_ocurrencia
           JOIN eventos e ON e.id_evento = o.id_evento
           WHERE r.id_reserva=?""",
        (int(d["id_reserva"]),),
    ).fetchone()
    if not f:
        return {"ok": False, "error": "RESERVA_NO_EXISTE"}
    seq = receptor.get_seq() if receptor is not None else obtener_seq_bd(conn)
    return {"ok": True, "data": dict(f), "seq": seq, "origen": server_id}


def obtener_ocurrencias_evento(
    conn: sqlite3.Connection, d: dict, server_id: str, receptor: ReceptorSeguidor | None
) -> dict:
    filas = conn.execute(
        """SELECT o.id_ocurrencia, o.id_evento, e.nombre, e.ocurrencias_mes,
                  o.fecha, o.asientos_totales, o.asientos_libres
           FROM ocurrencias o JOIN eventos e ON e.id_evento = o.id_evento
           WHERE o.id_evento=? ORDER BY o.fecha""",
        (d["id_evento"],),
    ).fetchall()
    if not filas:
        return {"ok": False, "error": "EVENTO_NO_EXISTE"}
    seq = receptor.get_seq() if receptor is not None else obtener_seq_bd(conn)
    return {"ok": True, "data": [dict(r) for r in filas], "seq": seq, "origen": server_id}


def confirmar_reserva(
    conn: sqlite3.Connection, d: dict, server_id: str, replicador: ReplicadorLider | None
) -> dict:
    n = int(d["num_asientos"])
    id_tx = str(d["id_tx"])
    if n <= 0:
        return {"ok": False, "error": "SOLICITUD_INVALIDA"}

    conn.execute("BEGIN IMMEDIATE")
    try:
        previa = _buscar_tx_previa(conn, id_tx)
        if previa:
            conn.execute("COMMIT")
            return previa

        o = conn.execute(
            "SELECT id_ocurrencia, asientos_totales, asientos_libres FROM ocurrencias "
            "WHERE id_evento=? AND fecha=?",
            (d["id_evento"], d["fecha"]),
        ).fetchone()
        if not o:
            conn.execute("ROLLBACK")
            return {"ok": False, "error": "EVENTO_NO_EXISTE"}
        if o["asientos_libres"] < n:
            conn.execute("ROLLBACK")
            return {"ok": False, "error": "SIN_DISPONIBILIDAD"}

        ahora = time.time()
        conn.execute(
            "UPDATE ocurrencias SET asientos_libres = asientos_libres - ? WHERE id_ocurrencia=?",
            (n, o["id_ocurrencia"]),
        )
        cur = conn.execute(
            "INSERT INTO reservas(id_tx, id_cliente, id_ocurrencia, num_asientos, estado, version, creada_en)"
            " VALUES (?,?,?,?, 'ACTIVA', 1, ?)",
            (id_tx, d["id_cliente"], o["id_ocurrencia"], n, ahora),
        )
        rid = int(cur.lastrowid)
        seq, cambios = _registrar_replicacion(
            conn, "confirmar_reserva", [o["id_ocurrencia"]], [rid], (id_tx, "confirmar_reserva", ahora)
        )
        resp = {
            "ok": True,
            "data": {
                "id_reserva": rid,
                "id_evento": d["id_evento"],
                "fecha": d["fecha"],
                "num_asientos": n,
                "asientos_libres": o["asientos_libres"] - n,
                "version": 1,
            },
            "seq": seq,
            "origen": server_id,
        }
        _guardar_tx(conn, id_tx, "confirmar_reserva", resp, seq, ahora)
        conn.execute("COMMIT")
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise

    if replicador is not None:
        replicador.propagar(seq, "confirmar_reserva", cambios)
    return resp


def modificar_reserva(
    conn: sqlite3.Connection, d: dict, server_id: str, replicador: ReplicadorLider | None
) -> dict:
    id_tx = str(d["id_tx"])
    id_reserva = int(d["id_reserva"])
    tipo = str(d.get("tipo", "")).upper()
    version_esperada = d.get("version_esperada")

    conn.execute("BEGIN IMMEDIATE")
    try:
        previa = _buscar_tx_previa(conn, id_tx)
        if previa:
            conn.execute("COMMIT")
            return previa

        r = conn.execute(
            """SELECT r.id_reserva, r.id_cliente, r.id_ocurrencia, r.num_asientos,
                      r.estado, r.version, o.id_evento, o.fecha,
                      o.asientos_totales, o.asientos_libres, e.ocurrencias_mes
               FROM reservas r
               JOIN ocurrencias o ON o.id_ocurrencia = r.id_ocurrencia
               JOIN eventos e ON e.id_evento = o.id_evento
               WHERE r.id_reserva=?""",
            (id_reserva,),
        ).fetchone()
        if not r:
            conn.execute("ROLLBACK")
            return {"ok": False, "error": "RESERVA_NO_EXISTE"}
        if r["estado"] != "ACTIVA":
            conn.execute("ROLLBACK")
            return {"ok": False, "error": "RESERVA_NO_ACTIVA"}
        if version_esperada is not None and int(version_esperada) != int(r["version"]):
            conn.execute("ROLLBACK")
            return {"ok": False, "error": "CONFLICTO_VERSION"}

        ocurrencias_tocadas = [r["id_ocurrencia"]]
        ahora = time.time()

        if tipo == "ASIENTOS":
            nuevo_n = int(d["nuevo_num_asientos"])
            if nuevo_n < 1 or nuevo_n > r["asientos_totales"]:
                conn.execute("ROLLBACK")
                return {"ok": False, "error": "SOLICITUD_INVALIDA"}
            delta = nuevo_n - int(r["num_asientos"])
            if delta > 0 and r["asientos_libres"] < delta:
                conn.execute("ROLLBACK")
                return {"ok": False, "error": "SIN_DISPONIBILIDAD"}
            conn.execute(
                "UPDATE ocurrencias SET asientos_libres = asientos_libres - ? WHERE id_ocurrencia=?",
                (delta, r["id_ocurrencia"]),
            )
            conn.execute(
                "UPDATE reservas SET num_asientos=?, version = version + 1 WHERE id_reserva=?",
                (nuevo_n, id_reserva),
            )
            nueva_fecha = r["fecha"]
            libres_final = int(r["asientos_libres"]) - delta
        elif tipo == "FECHA":
            if int(r["ocurrencias_mes"]) <= 1:
                conn.execute("ROLLBACK")
                return {"ok": False, "error": "SOLO_UNA_OCURRENCIA"}
            nueva_fecha = str(d["nueva_fecha"])
            if nueva_fecha == r["fecha"] or nueva_fecha[:7] != r["fecha"][:7]:
                conn.execute("ROLLBACK")
                return {"ok": False, "error": "FECHA_INVALIDA"}
            dest = conn.execute(
                "SELECT id_ocurrencia, asientos_libres FROM ocurrencias "
                "WHERE id_evento=? AND fecha=?",
                (r["id_evento"], nueva_fecha),
            ).fetchone()
            if not dest:
                conn.execute("ROLLBACK")
                return {"ok": False, "error": "EVENTO_NO_EXISTE"}
            n = int(r["num_asientos"])
            if dest["asientos_libres"] < n:
                conn.execute("ROLLBACK")
                return {"ok": False, "error": "SIN_DISPONIBILIDAD"}
            conn.execute(
                "UPDATE ocurrencias SET asientos_libres = asientos_libres + ? WHERE id_ocurrencia=?",
                (n, r["id_ocurrencia"]),
            )
            conn.execute(
                "UPDATE ocurrencias SET asientos_libres = asientos_libres - ? WHERE id_ocurrencia=?",
                (n, dest["id_ocurrencia"]),
            )
            conn.execute(
                "UPDATE reservas SET id_ocurrencia=?, version = version + 1 WHERE id_reserva=?",
                (dest["id_ocurrencia"], id_reserva),
            )
            ocurrencias_tocadas.append(dest["id_ocurrencia"])
            nuevo_n = n
            libres_final = int(dest["asientos_libres"]) - n
        else:
            conn.execute("ROLLBACK")
            return {"ok": False, "error": "TIPO_MODIFICACION_INVALIDO"}

        seq, cambios = _registrar_replicacion(
            conn,
            "modificar_reserva",
            ocurrencias_tocadas,
            [id_reserva],
            (id_tx, "modificar_reserva", ahora),
        )
        resp = {
            "ok": True,
            "data": {
                "id_reserva": id_reserva,
                "id_evento": r["id_evento"],
                "fecha": nueva_fecha,
                "num_asientos": nuevo_n,
                "asientos_libres": libres_final,
                "version": int(r["version"]) + 1,
                "tipo": tipo,
            },
            "seq": seq,
            "origen": server_id,
        }
        _guardar_tx(conn, id_tx, "modificar_reserva", resp, seq, ahora)
        conn.execute("COMMIT")
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise

    if replicador is not None:
        replicador.propagar(seq, "modificar_reserva", cambios)
    return resp


def cancelar_reserva(
    conn: sqlite3.Connection, d: dict, server_id: str, replicador: ReplicadorLider | None
) -> dict:
    id_tx = str(d["id_tx"])
    id_reserva = int(d["id_reserva"])

    conn.execute("BEGIN IMMEDIATE")
    try:
        previa = _buscar_tx_previa(conn, id_tx)
        if previa:
            conn.execute("COMMIT")
            return previa

        r = conn.execute(
            """SELECT r.id_reserva, r.id_cliente, r.id_ocurrencia, r.num_asientos,
                      r.estado, r.version, o.id_evento, o.fecha, o.asientos_libres
               FROM reservas r JOIN ocurrencias o ON o.id_ocurrencia = r.id_ocurrencia
               WHERE r.id_reserva=?""",
            (id_reserva,),
        ).fetchone()
        if not r:
            conn.execute("ROLLBACK")
            return {"ok": False, "error": "RESERVA_NO_EXISTE"}
        if r["estado"] != "ACTIVA":
            conn.execute("ROLLBACK")
            return {"ok": False, "error": "RESERVA_YA_CANCELADA"}

        ahora = time.time()
        n = int(r["num_asientos"])
        conn.execute(
            "UPDATE ocurrencias SET asientos_libres = asientos_libres + ? WHERE id_ocurrencia=?",
            (n, r["id_ocurrencia"]),
        )
        conn.execute(
            "UPDATE reservas SET estado='CANCELADA', version = version + 1 WHERE id_reserva=?",
            (id_reserva,),
        )
        seq, cambios = _registrar_replicacion(
            conn,
            "cancelar_reserva",
            [r["id_ocurrencia"]],
            [id_reserva],
            (id_tx, "cancelar_reserva", ahora),
        )
        resp = {
            "ok": True,
            "data": {
                "id_reserva": id_reserva,
                "id_evento": r["id_evento"],
                "fecha": r["fecha"],
                "asientos_liberados": n,
                "asientos_libres": int(r["asientos_libres"]) + n,
                "estado": "CANCELADA",
            },
            "seq": seq,
            "origen": server_id,
        }
        _guardar_tx(conn, id_tx, "cancelar_reserva", resp, seq, ahora)
        conn.execute("COMMIT")
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise

    if replicador is not None:
        replicador.propagar(seq, "cancelar_reserva", cambios)
    return resp


OPS_LECTURA = {
    "consultar_eventos": consultar_eventos,
    "disponibilidad": disponibilidad,
    "obtener_reserva": obtener_reserva,
    "obtener_ocurrencias_evento": obtener_ocurrencias_evento,
}

OPS_ESCRITURA = {
    "confirmar_reserva": confirmar_reserva,
    "modificar_reserva": modificar_reserva,
    "cancelar_reserva": cancelar_reserva,
}


def main() -> None:
    ap = argparse.ArgumentParser(description="Servicio de Persistencia SRB (Líder/Seguidor)")
    ap.add_argument("--rol", choices=["lider", "seguidor"], default="lider")
    ap.add_argument("--id", default=None, help="Identificador lógico de la réplica (ej. lider, seg1)")
    ap.add_argument("--puerto", type=int, default=None, help="Puerto REP para consultas/escrituras")
    ap.add_argument("--db", default=None, help="Ruta archivo SQLite")
    ap.add_argument(
        "--modo-repl",
        choices=["semi-sincrono", "sincrono", "asincrono", "none"],
        default=MODO_REPLICACION,
    )
    ap.add_argument("--lider-host", default=PERSIST_HOST)
    ap.add_argument("--repl-port", type=int, default=PERSIST_REPL_PORT)
    ap.add_argument("--catchup-port", type=int, default=PERSIST_CATCHUP_PORT)
    ap.add_argument("--hb-port", type=int, default=PERSIST_HB_PORT)
    ap.add_argument("--reset", action="store_true", help="Borra y recrea la BD inicial")
    ap.add_argument("--eventos", type=int, default=50)
    ap.add_argument("--dobles", type=int, default=20)
    ap.add_argument("--asientos", type=int, default=ASIENTOS_POR_EVENTO)
    ap.add_argument("--mes", default=MES_BASE)
    ap.add_argument("--seed-file", default=None, help="JSON opcional con escenario de sembrado")
    ap.add_argument("--metricas", default=None, help="Ruta CSV para métricas de BD")
    args = ap.parse_args()

    server_id = args.id or args.rol
    puerto = args.puerto or (PERSIST_PORT if args.rol == "lider" else 5567)
    db_path = args.db or (DB_PATH if args.rol == "lider" else f"srb_{server_id}.db")

    if args.reset:
        borrar_archivos_bd(db_path)

    conn = abrir_conexion(db_path)
    replicador: ReplicadorLider | None = None
    receptor: ReceptorSeguidor | None = None

    if args.rol == "lider":
        sembrar(conn, args.mes, args.eventos, args.dobles, args.asientos, args.seed_file)
        replicador = ReplicadorLider(
            db_path=db_path,
            server_id=server_id,
            modo_repl=args.modo_repl,
            repl_port=args.repl_port,
            catchup_port=args.catchup_port,
            hb_port=args.hb_port,
            ack_timeout_ms=REPL_ACK_TIMEOUT_MS,
        )
        replicador.iniciar(obtener_seq_bd(conn))
        print(
            f"[PERSISTENCIA:{server_id}] LIDER listo en :{puerto} (repl=:{args.repl_port}, "
            f"catchup=:{args.catchup_port}, modo={args.modo_repl}) | BD={db_path}",
            flush=True,
        )
        print(f"[PERSISTENCIA:{server_id}] Estado inicial: {resumen(conn)}", flush=True)
    else:
        receptor = ReceptorSeguidor(
            db_path=db_path,
            server_id=server_id,
            lider_host=args.lider_host,
            repl_port=args.repl_port,
            catchup_port=args.catchup_port,
        )
        receptor.iniciar()
        print(
            f"[PERSISTENCIA:{server_id}] SEGUIDOR listo en :{puerto} "
            f"(lider={args.lider_host}:{args.repl_port}) | BD={db_path}",
            flush=True,
        )
        print(f"[PERSISTENCIA:{server_id}] Estado sincronizado: {resumen(conn)}", flush=True)

    metricas_path = args.metricas or f"persist_metricas_{server_id}.csv"
    archivo_nuevo = not os.path.exists(metricas_path) or os.path.getsize(metricas_path) == 0
    f_met = open(metricas_path, "a", newline="", encoding="utf-8")
    w_met = csv.writer(f_met)
    if archivo_nuevo:
        w_met.writerow(["t_epoch", "id", "rol", "op", "latencia_ms", "ok"])
        f_met.flush()

    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.REP)
    sock.setsockopt(zmq.LINGER, 0)
    sock.bind(f"tcp://*:{puerto}")

    ops_desde_flush = 0
    try:
        while True:
            msg = sock.recv_json()
            t0 = time.perf_counter()
            op = msg.get("op", "")
            try:
                if op in OPS_LECTURA:
                    resp = OPS_LECTURA[op](conn, msg, server_id, receptor)
                elif op in OPS_ESCRITURA:
                    if args.rol != "lider":
                        resp = {"ok": False, "error": "SOLO_LECTURA_SEGUIDOR"}
                    else:
                        resp = OPS_ESCRITURA[op](conn, msg, server_id, replicador)
                else:
                    resp = {"ok": False, "error": f"OP_NO_SOPORTADA:{op}"}
            except KeyError as exc:
                resp = {"ok": False, "error": f"PETICION_INVALIDA:{exc}"}
            except Exception as exc:
                resp = {"ok": False, "error": f"ERROR_BD:{exc}"}

            lat_ms = (time.perf_counter() - t0) * 1000.0
            sock.send_json(resp)

            w_met.writerow(
                [f"{time.time():.3f}", server_id, args.rol, op, f"{lat_ms:.3f}", bool(resp.get("ok"))]
            )
            ops_desde_flush += 1
            if ops_desde_flush >= 20 or op in OPS_ESCRITURA:
                f_met.flush()
                ops_desde_flush = 0

            if op in OPS_ESCRITURA:
                print(
                    f"[PERSISTENCIA:{server_id}] {op} reserva/evento={msg.get('id_reserva') or msg.get('id_evento')} "
                    f"-> {resp.get('error', 'OK')} (seq={resp.get('seq', '-')}) | {resumen(conn)}",
                    flush=True,
                )
    except KeyboardInterrupt:
        pass
    finally:
        if replicador is not None:
            replicador.detener()
        if receptor is not None:
            receptor.detener()
        f_met.flush()
        f_met.close()
        sock.close()
        conn.close()


if __name__ == "__main__":
    main()
