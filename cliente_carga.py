"""Generador de carga parametrizable y ejecutor de casos de prueba del SRB.

Características:
- **Patrón síncrono (REQ -> ROUTER)**: para ``CONSULTA``, ``RESERVA`` y ``MODIFICACION``.
- **Patrón asíncrono (DEALER -> ROUTER + receptor PULL)**: para ``CANCELACION``,
  enviando el ``notif_endpoint`` propio y recibiendo notificaciones asíncronas en un hilo.
- **Tolerancia a fallos en el cliente (Lazy Pirate + Failover)**: si el GT Primario cae,
  conmuta al GT Backup y reintenta con el **mismo ``id_tx``** sin perder el estado ni
  reiniciar desde el principio.
- **Consistencia Centrada en el Cliente**: rastrea ``ultimo_seq`` y lo envía como
  ``min_seq`` en cada consulta (*Read-Your-Writes* y *Monotonic Reads*).
"""
import argparse
import csv
import json
import os
import random
import socket
import threading
import time
import uuid
import zmq

from config import (
    CLIENT_HOST,
    CLIENT_MAX_REINTENTOS,
    CLIENT_TIMEOUT_MS,
    GT_ASYNC_PORT,
    GT_BACKUP_ASYNC_PORT,
    GT_BACKUP_FRONT_PORT,
    GT_BACKUP_HOST,
    GT_FRONT_PORT,
    GT_HOST,
    MES_BASE,
)


def detectar_ip_local(gt_host: str, gt_port: int) -> str:
    """Determina la IP accesible desde M2 para que ``cancelaciones.py`` envíe notificaciones."""
    if CLIENT_HOST:
        return CLIENT_HOST
    if gt_host in ("127.0.0.1", "localhost", "::1"):
        return "127.0.0.1"
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect((gt_host, gt_port))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()


class ReceptorNotificaciones:
    """Escucha notificaciones PUSH asíncronas de cancelación en un hilo dedicado."""

    def __init__(self, ctx: zmq.Context, cid: str, ip_local: str, puerto: int = 0):
        self.ctx = ctx
        self.cid = cid
        self.sock = ctx.socket(zmq.PULL)
        self.sock.setsockopt(zmq.LINGER, 0)
        self.sock.setsockopt(zmq.RCVTIMEO, 250)
        if puerto > 0:
            self.sock.bind(f"tcp://*:{puerto}")
            puerto_real = puerto
        else:
            puerto_real = self.sock.bind_to_random_port("tcp://*")
        self.endpoint = f"tcp://{ip_local}:{puerto_real}"
        self.lock = threading.Lock()
        self.pendientes: dict[str, dict] = {}
        self.recibidas: list[dict] = []
        self.ultimo_seq = 0
        self._stop = threading.Event()
        self._hilo = threading.Thread(target=self._loop, daemon=True)
        self._hilo.start()

    def registrar_envio(self, id_tx: str, id_reserva: int, t_perf: float) -> threading.Event:
        ev = threading.Event()
        with self.lock:
            self.pendientes[id_tx] = {"id_reserva": id_reserva, "t_perf": t_perf, "ev": ev}
        return ev

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                msg = self.sock.recv_json()
            except zmq.Again:
                continue
            except zmq.ZMQError:
                break

            id_tx = str(msg.get("id_tx", ""))
            seq = int(msg.get("seq", 0) or 0)
            lat_notif_ms = 0.0
            with self.lock:
                self.ultimo_seq = max(self.ultimo_seq, seq)
                info = self.pendientes.pop(id_tx, None)
                if info is not None:
                    lat_notif_ms = (time.perf_counter() - info["t_perf"]) * 1000.0
                msg["latencia_notif_ms"] = lat_notif_ms
                self.recibidas.append(msg)
                if info is not None:
                    info["notif"] = msg
                    info["ev"].set()
            print(
                f"[NOTIF {self.cid}] Cancelación reserva={msg.get('id_reserva')} "
                f"-> {msg.get('estado')} (seq={seq}, {lat_notif_ms:.1f} ms)",
                flush=True,
            )

    def esperar_todas(self, timeout_s: float = 3.0) -> None:
        deadline = time.perf_counter() + timeout_s
        while time.perf_counter() < deadline:
            with self.lock:
                if not self.pendientes:
                    return
            time.sleep(0.05)

    def cerrar(self) -> None:
        self._stop.set()
        self._hilo.join(timeout=1.0)
        self.sock.close()


class ClienteSRB:
    """Cliente con patrón Lazy Pirate y failover automático entre GT Primario y Backup."""

    def __init__(
        self,
        cid: str,
        timeout_ms: int = CLIENT_TIMEOUT_MS,
        solo_primario: bool = False,
        notif_port: int = 0,
        max_reintentos: int = CLIENT_MAX_REINTENTOS,
    ):
        self.cid = cid
        self.timeout_ms = timeout_ms
        self.max_reintentos = max_reintentos
        self.ctx = zmq.Context.instance()
        self.gts = [{"host": GT_HOST, "front": GT_FRONT_PORT, "async": GT_ASYNC_PORT}]
        if not solo_primario and (GT_BACKUP_HOST, GT_BACKUP_FRONT_PORT) != (
            GT_HOST,
            GT_FRONT_PORT,
        ):
            self.gts.append(
                {
                    "host": GT_BACKUP_HOST,
                    "front": GT_BACKUP_FRONT_PORT,
                    "async": GT_BACKUP_ASYNC_PORT,
                }
            )
        self.gt_idx = 0
        self.ultimo_seq = 0
        ip_local = detectar_ip_local(GT_HOST, GT_FRONT_PORT)
        self.notif = ReceptorNotificaciones(self.ctx, cid, ip_local, notif_port)
        self.sock_sync = self._conectar_sync()
        self.sock_async = self._conectar_async()

    def _conectar_sync(self) -> zmq.Socket:
        gt = self.gts[self.gt_idx]
        s = self.ctx.socket(zmq.REQ)
        s.setsockopt(zmq.RCVTIMEO, self.timeout_ms)
        s.setsockopt(zmq.SNDTIMEO, self.timeout_ms)
        s.setsockopt(zmq.LINGER, 0)
        s.connect(f"tcp://{gt['host']}:{gt['front']}")
        return s

    def _conectar_async(self) -> zmq.Socket:
        gt = self.gts[self.gt_idx]
        s = self.ctx.socket(zmq.DEALER)
        s.setsockopt(zmq.RCVTIMEO, self.timeout_ms)
        s.setsockopt(zmq.SNDTIMEO, self.timeout_ms)
        s.setsockopt(zmq.LINGER, 0)
        s.connect(f"tcp://{gt['host']}:{gt['async']}")
        return s

    def _rotar_gt(self) -> None:
        self.gt_idx = (self.gt_idx + 1) % len(self.gts)
        self.sock_sync.close()
        self.sock_async.close()
        self.sock_sync = self._conectar_sync()
        self.sock_async = self._conectar_async()

    def _seq_actual(self) -> int:
        with self.notif.lock:
            self.ultimo_seq = max(self.ultimo_seq, self.notif.ultimo_seq)
        return self.ultimo_seq

    def pedir_sincrono(self, op: str, payload: dict) -> tuple[str, float, dict, int]:
        """Envía operación síncrona con reintentos idempotentes ante fallo de GT o RMC."""
        req_payload = dict(payload)
        if op == "CONSULTA":
            req_payload["min_seq"] = max(int(req_payload.get("min_seq", 0)), self._seq_actual())

        t0 = time.perf_counter()
        reintentos = 0
        for intento in range(self.max_reintentos):
            try:
                self.sock_sync.send_json({"op": op, "payload": req_payload})
                resp = self.sock_sync.recv_json()
                if (
                    resp.get("estado") == "ERROR"
                    and resp.get("detalle") == "SERVICIO_NO_DISPONIBLE"
                    and intento < self.max_reintentos - 1
                ):
                    raise zmq.Again()
                seq = int(resp.get("seq", 0) or 0)
                if seq > 0:
                    self.ultimo_seq = max(self.ultimo_seq, seq)
                return resp["estado"], (time.perf_counter() - t0) * 1000.0, resp, reintentos
            except zmq.Again:
                reintentos += 1
                if intento < self.max_reintentos - 1:
                    self._rotar_gt()
                    gt = self.gts[self.gt_idx]
                    print(
                        f"[CLIENTE {self.cid}] Reintento #{reintentos} op={op} "
                        f"-> GT {gt['host']}:{gt['front']}",
                        flush=True,
                    )
                    time.sleep(0.15)
                else:
                    self._rotar_gt()

        return (
            "TIMEOUT",
            (time.perf_counter() - t0) * 1000.0,
            {"estado": "TIMEOUT", "detalle": None},
            reintentos,
        )

    def pedir_cancelacion(self, payload: dict) -> tuple[str, float, dict, int, threading.Event]:
        """Envía cancelación asíncrona no bloqueante y retorna evento para esperar notificación."""
        req_payload = dict(payload)
        req_payload.setdefault("id_tx", str(uuid.uuid4()))
        req_payload.setdefault("id_cliente", self.cid)
        req_payload["notif_endpoint"] = self.notif.endpoint

        t0 = time.perf_counter()
        ev = self.notif.registrar_envio(req_payload["id_tx"], int(req_payload["id_reserva"]), t0)
        reintentos = 0

        for intento in range(self.max_reintentos):
            try:
                self.sock_async.send_multipart(
                    [b"", json.dumps({"op": "CANCELACION", "payload": req_payload}).encode("utf-8")]
                )
                frames = self.sock_async.recv_multipart()
                resp = json.loads(frames[-1])
                if resp.get("estado") == "ACEPTADA":
                    return (
                        "ACEPTADA",
                        (time.perf_counter() - t0) * 1000.0,
                        resp,
                        reintentos,
                        ev,
                    )
                if intento < self.max_reintentos - 1:
                    raise zmq.Again()
                return (
                    resp.get("estado", "ERROR"),
                    (time.perf_counter() - t0) * 1000.0,
                    resp,
                    reintentos,
                    ev,
                )
            except (zmq.Again, ValueError):
                reintentos += 1
                if intento < self.max_reintentos - 1:
                    self._rotar_gt()
                    gt = self.gts[self.gt_idx]
                    print(
                        f"[CLIENTE {self.cid}] Reintento CANCELACION #{reintentos} "
                        f"-> GT {gt['host']}:{gt['async']}",
                        flush=True,
                    )
                    time.sleep(0.15)
                else:
                    self._rotar_gt()

        return (
            "TIMEOUT",
            (time.perf_counter() - t0) * 1000.0,
            {"estado": "TIMEOUT", "detalle": None},
            reintentos,
            ev,
        )

    def cerrar(self) -> None:
        self.sock_sync.close()
        self.sock_async.close()
        self.notif.cerrar()


def _construir_mapa_fechas(catalogo: list[dict]) -> dict[str, list[str]]:
    mapa: dict[str, list[str]] = {}
    for o in catalogo:
        mapa.setdefault(o["id_evento"], [])
        if o["fecha"] not in mapa[o["id_evento"]]:
            mapa[o["id_evento"]].append(o["fecha"])
    return mapa


def ejecutar_carga(
    args: argparse.Namespace,
    cid: str,
    notif_port: int,
    filas_out: list,
    notifs_out: list,
) -> None:
    """Bucle generador de carga parametrizable para un hilo/cliente."""
    rnd = random.Random(f"{args.semilla}-{cid}")
    cli = ClienteSRB(
        cid,
        timeout_ms=args.timeout_ms,
        solo_primario=args.solo_primario,
        notif_port=notif_port,
    )
    try:
        _, _, r, _ = cli.pedir_sincrono("CONSULTA", {"mes": args.mes})
        catalogo = r["detalle"] if isinstance(r.get("detalle"), list) else []
        mapa_fechas = _construir_mapa_fechas(catalogo)
        reservas_activas: list[dict] = []

        pesos = [
            ("CONSULTA", args.pct_consulta),
            ("RESERVA", args.pct_reserva),
            ("MODIFICACION", args.pct_modificacion),
            ("CANCELACION", args.pct_cancelacion),
        ]
        ops_validas = [op for op, w in pesos if w > 0]
        w_validos = [w for _, w in pesos if w > 0]
        if not ops_validas:
            ops_validas, w_validos = ["CONSULTA"], [100.0]

        intervalo = (60.0 / args.rpm) if args.rpm > 0 else (args.pausa_ms / 1000.0)
        fin = time.time() + args.duracion
        siguiente = time.time()

        while time.time() < fin:
            if intervalo > 0:
                espera = siguiente - time.time()
                if espera > 0:
                    time.sleep(espera)
                siguiente = max(siguiente + intervalo, time.time())

            op = rnd.choices(ops_validas, weights=w_validos, k=1)[0]
            if not catalogo:
                op = "CONSULTA"
            elif op in ("MODIFICACION", "CANCELACION") and not reservas_activas:
                op = "RESERVA"

            t_envio = time.time()

            if op == "CONSULTA":
                estado, lat, resp, reint = cli.pedir_sincrono("CONSULTA", {"mes": args.mes})
                if estado == "EXITO" and isinstance(resp.get("detalle"), list):
                    catalogo = resp["detalle"]
                    mapa_fechas = _construir_mapa_fechas(catalogo)
                filas_out.append((cid, f"{t_envio:.3f}", op, f"{lat:.3f}", estado, reint))

            elif op == "RESERVA":
                disponibles = [o for o in catalogo if o["asientos_libres"] > 0] or catalogo
                o = rnd.choice(disponibles)
                n = rnd.randint(1, args.max_asientos)
                payload = {
                    "id_tx": str(uuid.uuid4()),
                    "id_cliente": cid,
                    "id_evento": o["id_evento"],
                    "fecha": o["fecha"],
                    "num_asientos": n,
                }
                estado, lat, resp, reint = cli.pedir_sincrono("RESERVA", payload)
                if estado == "EXITO" and isinstance(resp.get("detalle"), dict):
                    det = resp["detalle"]
                    reservas_activas.append(
                        {
                            "id_reserva": det["id_reserva"],
                            "id_evento": o["id_evento"],
                            "fecha": o["fecha"],
                            "num_asientos": n,
                            "ocurrencias_mes": int(o.get("ocurrencias_mes", 1)),
                            "version": int(det.get("version", 1)),
                        }
                    )
                    if len(reservas_activas) > 200:
                        reservas_activas.pop(0)
                filas_out.append((cid, f"{t_envio:.3f}", op, f"{lat:.3f}", estado, reint))

            elif op == "MODIFICACION":
                res = rnd.choice(reservas_activas)
                fechas_ev = [
                    f for f in mapa_fechas.get(res["id_evento"], []) if f != res["fecha"]
                ]
                if res["ocurrencias_mes"] > 1 and fechas_ev and rnd.random() < 0.5:
                    nueva_fecha = rnd.choice(fechas_ev)
                    payload = {
                        "id_tx": str(uuid.uuid4()),
                        "id_reserva": res["id_reserva"],
                        "tipo": "FECHA",
                        "nueva_fecha": nueva_fecha,
                        "version_esperada": res["version"],
                    }
                else:
                    nuevo_n = rnd.randint(1, args.max_asientos)
                    payload = {
                        "id_tx": str(uuid.uuid4()),
                        "id_reserva": res["id_reserva"],
                        "tipo": "ASIENTOS",
                        "nuevo_num_asientos": nuevo_n,
                        "version_esperada": res["version"],
                    }
                estado, lat, resp, reint = cli.pedir_sincrono("MODIFICACION", payload)
                if estado == "EXITO" and isinstance(resp.get("detalle"), dict):
                    det = resp["detalle"]
                    res["fecha"] = det.get("fecha", res["fecha"])
                    res["num_asientos"] = det.get("num_asientos", res["num_asientos"])
                    res["version"] = int(det.get("version", res["version"] + 1))
                elif resp.get("detalle") in ("RESERVA_NO_ACTIVA", "RESERVA_NO_EXISTE"):
                    if res in reservas_activas:
                        reservas_activas.remove(res)
                filas_out.append((cid, f"{t_envio:.3f}", op, f"{lat:.3f}", estado, reint))

            elif op == "CANCELACION":
                idx = rnd.randrange(len(reservas_activas))
                res = reservas_activas.pop(idx)
                payload = {
                    "id_tx": str(uuid.uuid4()),
                    "id_cliente": cid,
                    "id_reserva": res["id_reserva"],
                }
                estado, lat, _, reint, _ = cli.pedir_cancelacion(payload)
                filas_out.append((cid, f"{t_envio:.3f}", op, f"{lat:.3f}", estado, reint))

        cli.notif.esperar_todas(timeout_s=3.0)
        with cli.notif.lock:
            for n in cli.notif.recibidas:
                notifs_out.append(
                    (
                        cid,
                        n.get("id_tx"),
                        n.get("id_reserva"),
                        n.get("estado"),
                        n.get("seq"),
                        f"{n.get('latencia_notif_ms', 0.0):.3f}",
                    )
                )
    finally:
        cli.cerrar()


def ejecutar_script(ruta_script: str, args: argparse.Namespace) -> None:
    """Ejecuta un archivo JSON determinístico de operaciones para sustentación."""
    with open(ruta_script, "r", encoding="utf-8") as f:
        plan = json.load(f)

    cid = args.cliente_id or plan.get("cliente_id", "PROF-1")
    mes = plan.get("mes", args.mes)
    cli = ClienteSRB(
        cid,
        timeout_ms=args.timeout_ms,
        solo_primario=args.solo_primario,
        notif_port=args.notif_port_base,
    )
    refs: dict[str, dict] = {}
    eventos_notif: list[threading.Event] = []

    print(f"=== Ejecutando script {ruta_script} como cliente {cid} ===", flush=True)
    try:
        for i, paso in enumerate(plan.get("pasos", []), 1):
            op = paso["op"].upper()
            if paso.get("esperar_ms", 0) > 0:
                time.sleep(paso["esperar_ms"] / 1000.0)

            if op == "CONSULTA":
                est, lat, resp, _ = cli.pedir_sincrono("CONSULTA", {"mes": paso.get("mes", mes)})
                n_ev = len(resp["detalle"]) if isinstance(resp.get("detalle"), list) else 0
                print(
                    f"[{i}] CONSULTA -> {est} ({n_ev} ocurrencias, seq={resp.get('seq')}, "
                    f"bd={resp.get('origen_bd')}, {lat:.1f} ms)",
                    flush=True,
                )

            elif op == "RESERVA":
                payload = {
                    "id_tx": paso.get("id_tx", str(uuid.uuid4())),
                    "id_cliente": paso.get("id_cliente", cid),
                    "id_evento": paso["id_evento"],
                    "fecha": paso["fecha"],
                    "num_asientos": int(paso["num_asientos"]),
                }
                est, lat, resp, _ = cli.pedir_sincrono("RESERVA", payload)
                det = resp.get("detalle")
                if est == "EXITO" and paso.get("guardar_como") and isinstance(det, dict):
                    refs[paso["guardar_como"]] = {
                        "id_reserva": det["id_reserva"],
                        "version": det.get("version", 1),
                    }
                print(
                    f"[{i}] RESERVA {paso['id_evento']} {paso['fecha']} n={paso['num_asientos']} "
                    f"-> {est} | {det} ({lat:.1f} ms)",
                    flush=True,
                )

            elif op == "MODIFICACION":
                ref = paso.get("ref")
                if ref and ref in refs:
                    id_res = refs[ref]["id_reserva"]
                elif "id_reserva" in paso:
                    id_res = paso["id_reserva"]
                else:
                    print(f"[{i}] MODIFICACION omitida: referencia '{ref}' no existe", flush=True)
                    continue
                payload = {
                    "id_tx": paso.get("id_tx", str(uuid.uuid4())),
                    "id_reserva": id_res,
                    "tipo": paso["tipo"].upper(),
                }
                if payload["tipo"] == "ASIENTOS":
                    payload["nuevo_num_asientos"] = int(paso["nuevo_num_asientos"])
                else:
                    payload["nueva_fecha"] = paso["nueva_fecha"]
                est, lat, resp, _ = cli.pedir_sincrono("MODIFICACION", payload)
                print(
                    f"[{i}] MODIFICACION #{id_res} ({payload['tipo']}) -> {est} | "
                    f"{resp.get('detalle')} ({lat:.1f} ms)",
                    flush=True,
                )

            elif op == "CANCELACION":
                ref = paso.get("ref")
                if ref and ref in refs:
                    id_res = refs[ref]["id_reserva"]
                elif "id_reserva" in paso:
                    id_res = paso["id_reserva"]
                else:
                    print(f"[{i}] CANCELACION omitida: referencia '{ref}' no existe", flush=True)
                    continue
                payload = {
                    "id_tx": paso.get("id_tx", str(uuid.uuid4())),
                    "id_cliente": cid,
                    "id_reserva": id_res,
                }
                est, lat, _, _, ev = cli.pedir_cancelacion(payload)
                eventos_notif.append(ev)
                print(
                    f"[{i}] CANCELACION #{id_res} -> ACK={est} ({lat:.1f} ms)",
                    flush=True,
                )

        for ev in eventos_notif:
            ev.wait(timeout=5.0)
        cli.notif.esperar_todas(timeout_s=2.0)
        print(
            f"=== Fin script {ruta_script} | Notificaciones recibidas: {len(cli.notif.recibidas)} ===",
            flush=True,
        )
    finally:
        cli.cerrar()


def main() -> None:
    ap = argparse.ArgumentParser(description="Cliente de carga y pruebas SRB")
    ap.add_argument("--cliente-id", default="C1")
    ap.add_argument("--hilos", type=int, default=1)
    ap.add_argument("--rpm", type=float, default=60, help="Solicitudes/min por hilo (>=20 o 0=máximo)")
    ap.add_argument("--pausa-ms", type=float, default=5.0, help="Pausa entre peticiones si --rpm 0")
    ap.add_argument("--duracion", type=int, default=150, help="Duración en segundos")
    ap.add_argument("--pct-consulta", type=float, default=100.0)
    ap.add_argument("--pct-reserva", type=float, default=None)
    ap.add_argument("--pct-modificacion", type=float, default=None)
    ap.add_argument("--pct-cancelacion", type=float, default=None)
    ap.add_argument("--max-asientos", type=int, default=4)
    ap.add_argument("--mes", default=MES_BASE)
    ap.add_argument("--semilla", default="42")
    ap.add_argument("--timeout-ms", type=int, default=CLIENT_TIMEOUT_MS)
    ap.add_argument("--salida", default=None)
    ap.add_argument("--notif-port-base", type=int, default=0, help="Puerto base PULL (0=aleatorio)")
    ap.add_argument("--solo-primario", action="store_true", help="Deshabilitar failover a GT Backup")
    ap.add_argument("--script", default=None, help="Ruta a archivo JSON determinístico")
    args = ap.parse_args()

    if args.script:
        ejecutar_script(args.script, args)
        return

    # Compatibilidad con Entrega 1: si solo se indica --pct-consulta, el resto son reservas
    if (
        args.pct_reserva is None
        and args.pct_modificacion is None
        and args.pct_cancelacion is None
    ):
        args.pct_reserva = max(0.0, 100.0 - args.pct_consulta)
        args.pct_modificacion = 0.0
        args.pct_cancelacion = 0.0
    else:
        args.pct_reserva = args.pct_reserva or 0.0
        args.pct_modificacion = args.pct_modificacion or 0.0
        args.pct_cancelacion = args.pct_cancelacion or 0.0

    if 0 < args.rpm < 20:
        print("AVISO: el enunciado exige al menos 20 solicitudes/min por cliente", flush=True)

    resultados: list[list] = []
    notificaciones: list[list] = []
    hilos: list[threading.Thread] = []

    for i in range(args.hilos):
        cid = args.cliente_id if args.hilos == 1 else f"{args.cliente_id}-{i}"
        filas: list = []
        notifs: list = []
        resultados.append(filas)
        notificaciones.append(notifs)
        puerto = (args.notif_port_base + i) if args.notif_port_base > 0 else 0
        t = threading.Thread(
            target=ejecutar_carga, args=(args, cid, puerto, filas, notifs)
        )
        t.start()
        hilos.append(t)

    for t in hilos:
        t.join()

    salida = args.salida or f"resultados_{args.cliente_id}.csv"
    carpeta = os.path.dirname(os.path.abspath(salida))
    if carpeta:
        os.makedirs(carpeta, exist_ok=True)
    with open(salida, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["cliente", "t_envio", "op", "latencia_ms", "estado", "reintentos"])
        for filas in resultados:
            w.writerows(filas)

    total_notifs = sum(len(n) for n in notificaciones)
    if total_notifs > 0:
        notif_path = os.path.join(
            os.path.dirname(os.path.abspath(salida)),
            f"notificaciones_{args.cliente_id}.csv",
        )
        with open(notif_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["cliente", "id_tx", "id_reserva", "estado", "seq", "latencia_notif_ms"])
            for ns in notificaciones:
                w.writerows(ns)

    total_reqs = sum(len(f) for f in resultados)
    print(f"[{args.cliente_id}] {total_reqs} solicitudes -> {salida} (notifs={total_notifs})", flush=True)


if __name__ == "__main__":
    main()
