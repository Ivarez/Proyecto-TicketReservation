"""Servicio RMC (Reservas, Modificaciones y Consultas) escalable y tolerante a fallos.

Características principales:
- **Stateless y replicable**: múltiples procesos ``rmc.py`` se conectan vía ``DEALER``
  al ``ROUTER`` backend del GT (patrón *Paranoid Pirate*).
- **Failover automático de GT**: si el GT Primario deja de enviar ``HB_PING``/``HB_ACK``
  durante ``GT_FAILOVER_TIMEOUT_S``, el worker conmuta automáticamente hacia el GT Backup.
- **Read/Write Splitting**:
  - ``CONSULTA`` se balancea entre las réplicas Seguidoras (pasando ``min_seq`` para
    garantizar *Read-Your-Writes* y *Monotonic Reads*), con fallback automático a otra
    réplica o al Líder si alguna seguidora cae (excepto con ``--sin-replicas`` para la
    Arquitectura Base).
  - ``RESERVA`` y ``MODIFICACION`` siempre consultan disponibilidad en tiempo real y
    escriben en el **BD Líder** (garantizando consistencia fuerte CP y cero sobreventa).
"""
import argparse
import json
import os
import signal
import time
import zmq

from config import (
    ASIENTOS_POR_EVENTO,
    GT_BACK_PORT,
    GT_BACKUP_BACK_PORT,
    GT_BACKUP_HOST,
    GT_FAILOVER_TIMEOUT_S,
    GT_HOST,
    MES_BASE,
    PERSIST_HOST,
    PERSIST_PORT,
    RMC_HB_INTERVAL_S,
    parse_replicas,
)


class ClientePersistencia:
    """Administra conexiones REQ/REP tolerantes a timeout hacia Líder y Seguidoras."""

    def __init__(self, ctx: zmq.Context, sin_replicas: bool):
        self.ctx = ctx
        self.lider_ep = (PERSIST_HOST, PERSIST_PORT)
        self.replicas = [] if sin_replicas else parse_replicas()
        self.rr_idx = 0
        self.sock_lider = self._crear_req(*self.lider_ep)
        self.socks_rep: dict[tuple[str, int], zmq.Socket] = {
            ep: self._crear_req(*ep) for ep in self.replicas
        }

    def _crear_req(self, host: str, port: int) -> zmq.Socket:
        s = self.ctx.socket(zmq.REQ)
        s.setsockopt(zmq.RCVTIMEO, 2500)
        s.setsockopt(zmq.SNDTIMEO, 2500)
        s.setsockopt(zmq.LINGER, 0)
        s.connect(f"tcp://{host}:{port}")
        return s

    def _llamar_ep(self, ep: tuple[str, int], sock_map: dict, payload: dict) -> dict | None:
        sock = sock_map[ep]
        try:
            sock.send_json(payload)
            return sock.recv_json()
        except zmq.Again:
            sock.close()
            sock_map[ep] = self._crear_req(*ep)
            return None

    def llamar_lider(self, op: str, **datos) -> dict:
        payload = {"op": op, **datos}
        for intento in range(3):
            try:
                self.sock_lider.send_json(payload)
                return self.sock_lider.recv_json()
            except zmq.Again:
                self.sock_lider.close()
                self.sock_lider = self._crear_req(*self.lider_ep)
                if intento < 2:
                    time.sleep(0.1)
        return {"ok": False, "error": "BD_LIDER_NO_RESPONDE"}

    def consultar_lectura(self, op: str, **datos) -> dict:
        """Balancea lecturas entre Seguidoras con fallback a otra seguidora o al Líder."""
        if not self.replicas:
            return self.llamar_lider(op, **datos)

        payload = {"op": op, **datos}
        n = len(self.replicas)
        inicio = self.rr_idx % n
        self.rr_idx = (self.rr_idx + 1) % n

        for offset in range(n):
            ep = self.replicas[(inicio + offset) % n]
            resp = self._llamar_ep(ep, self.socks_rep, payload)
            if resp is not None and resp.get("ok"):
                return resp

        # Fallback final al Líder si todas las seguidoras están caídas
        return self.llamar_lider(op, **datos)

    def cerrar(self) -> None:
        self.sock_lider.close()
        for s in self.socks_rep.values():
            s.close()


def procesar(msg: dict, bd: ClientePersistencia) -> tuple[str, object, int, str]:
    """Ejecuta la lógica de negocio para ``CONSULTA``, ``RESERVA`` y ``MODIFICACION``."""
    op = msg.get("op")
    p = msg.get("payload", {})

    if op == "CONSULTA":
        r = bd.consultar_lectura(
            "consultar_eventos",
            mes=p.get("mes", MES_BASE),
            min_seq=p.get("min_seq", 0),
        )
        if r.get("ok"):
            return "EXITO", r["data"], int(r.get("seq", 0)), str(r.get("origen", "bd"))
        return "ERROR", r.get("error", "ERROR_CONSULTA"), 0, "bd"

    if op == "RESERVA":
        n = p.get("num_asientos")
        if not (
            p.get("id_tx")
            and p.get("id_cliente")
            and p.get("id_evento")
            and p.get("fecha")
            and isinstance(n, int)
            and 1 <= n <= ASIENTOS_POR_EVENTO
        ):
            return "RECHAZO", "SOLICITUD_INVALIDA", 0, "rmc"

        disp = bd.llamar_lider(
            "disponibilidad", id_evento=p["id_evento"], fecha=p["fecha"], id_tx=p["id_tx"]
        )
        if not disp.get("ok"):
            err = disp.get("error", "ERROR_BD")
            est = "RECHAZO" if err == "EVENTO_NO_EXISTE" else "ERROR"
            return est, err, int(disp.get("seq", 0)), "lider"
        if "tx_previa" in disp:
            prev = disp["tx_previa"]
            return "EXITO", prev["data"], int(prev.get("seq", 0)), str(prev.get("origen", "lider"))

        if disp["data"]["asientos_libres"] < n:
            return "RECHAZO", "SIN_DISPONIBILIDAD", int(disp.get("seq", 0)), "lider"

        r = bd.llamar_lider(
            "confirmar_reserva",
            id_tx=p["id_tx"],
            id_cliente=p["id_cliente"],
            id_evento=p["id_evento"],
            fecha=p["fecha"],
            num_asientos=n,
        )
        if r.get("ok"):
            return "EXITO", r["data"], int(r.get("seq", 0)), str(r.get("origen", "lider"))
        err = r.get("error", "ERROR_BD")
        est = "RECHAZO" if err in ("SIN_DISPONIBILIDAD", "EVENTO_NO_EXISTE") else "ERROR"
        return est, err, 0, "lider"

    if op == "MODIFICACION":
        id_tx = p.get("id_tx")
        id_reserva = p.get("id_reserva")
        tipo = str(p.get("tipo", "")).upper()
        if not id_tx or id_reserva is None or tipo not in ("ASIENTOS", "FECHA"):
            return "RECHAZO", "SOLICITUD_INVALIDA", 0, "rmc"

        # Paso 2 (Enunciado): consultar reserva actual y disponibilidad
        actual = bd.llamar_lider("obtener_reserva", id_reserva=id_reserva, id_tx=id_tx)
        if not actual.get("ok"):
            err = actual.get("error", "ERROR_BD")
            return ("RECHAZO" if err == "RESERVA_NO_EXISTE" else "ERROR"), err, 0, "lider"
        if "tx_previa" in actual:
            prev = actual["tx_previa"]
            return "EXITO", prev["data"], int(prev.get("seq", 0)), str(prev.get("origen", "lider"))

        info = actual["data"]
        if info["estado"] != "ACTIVA":
            return "RECHAZO", "RESERVA_NO_ACTIVA", int(actual.get("seq", 0)), "lider"

        if tipo == "ASIENTOS":
            nuevo_n = p.get("nuevo_num_asientos")
            if not isinstance(nuevo_n, int) or not (1 <= nuevo_n <= info["asientos_totales"]):
                return "RECHAZO", "SOLICITUD_INVALIDA", 0, "rmc"
            delta = nuevo_n - info["num_asientos"]
            if delta > 0 and info["asientos_libres"] < delta:
                return "RECHAZO", "SIN_DISPONIBILIDAD", int(actual.get("seq", 0)), "lider"
        else:
            # Cambio de fecha conservando el número de asientos
            nueva_fecha = str(p.get("nueva_fecha", ""))
            if info["ocurrencias_mes"] <= 1:
                return "RECHAZO", "SOLO_UNA_OCURRENCIA", int(actual.get("seq", 0)), "lider"
            if not nueva_fecha or nueva_fecha == info["fecha"] or nueva_fecha[:7] != info["fecha"][:7]:
                return "RECHAZO", "FECHA_INVALIDA", int(actual.get("seq", 0)), "lider"
            disp_dest = bd.llamar_lider(
                "disponibilidad", id_evento=info["id_evento"], fecha=nueva_fecha
            )
            if not disp_dest.get("ok"):
                err = disp_dest.get("error", "ERROR_BD")
                return ("RECHAZO" if err == "EVENTO_NO_EXISTE" else "ERROR"), err, 0, "lider"
            if disp_dest["data"]["asientos_libres"] < info["num_asientos"]:
                return "RECHAZO", "SIN_DISPONIBILIDAD", int(disp_dest.get("seq", 0)), "lider"

        # Paso 3: confirmar modificación atómica en BD Líder
        r = bd.llamar_lider(
            "modificar_reserva",
            id_tx=id_tx,
            id_reserva=id_reserva,
            tipo=tipo,
            nuevo_num_asientos=p.get("nuevo_num_asientos"),
            nueva_fecha=p.get("nueva_fecha"),
            version_esperada=p.get("version_esperada"),
        )
        if r.get("ok"):
            return "EXITO", r["data"], int(r.get("seq", 0)), str(r.get("origen", "lider"))
        err = r.get("error", "ERROR_BD")
        rechazos = {
            "RESERVA_NO_EXISTE",
            "RESERVA_NO_ACTIVA",
            "SIN_DISPONIBILIDAD",
            "SOLO_UNA_OCURRENCIA",
            "FECHA_INVALIDA",
            "EVENTO_NO_EXISTE",
            "CONFLICTO_VERSION",
            "SOLICITUD_INVALIDA",
        }
        return ("RECHAZO" if err in rechazos else "ERROR"), err, 0, "lider"

    return "ERROR", "OPERACION_NO_SOPORTADA", 0, "rmc"


def main() -> None:
    ap = argparse.ArgumentParser(description="Servicio RMC SRB")
    ap.add_argument("--worker-id", default=f"rmc-{os.getpid()}")
    ap.add_argument(
        "--sin-replicas",
        action="store_true",
        help="Enviar consultas directo al Líder (Arquitectura Base)",
    )
    ap.add_argument(
        "--solo-primario",
        action="store_true",
        help="Conectar únicamente al GT Primario (Arquitectura Base)",
    )
    args = ap.parse_args()

    ctx = zmq.Context.instance()
    bd = ClientePersistencia(ctx, sin_replicas=args.sin_replicas)

    gts = [(GT_HOST, GT_BACK_PORT)]
    if not args.solo_primario:
        gts.append((GT_BACKUP_HOST, GT_BACKUP_BACK_PORT))

    gt_idx = 0
    ejecutando = True

    def _parar(*_):
        nonlocal ejecutando
        ejecutando = False

    signal.signal(signal.SIGINT, _parar)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _parar)

    def conectar_gt(idx: int) -> zmq.Socket:
        host, port = gts[idx]
        s = ctx.socket(zmq.DEALER)
        s.setsockopt(zmq.LINGER, 0)
        s.setsockopt(zmq.RECONNECT_IVL, 100)
        s.setsockopt(zmq.RECONNECT_IVL_MAX, 500)
        s.setsockopt(zmq.IDENTITY, f"{args.worker_id}-{os.getpid()}-{idx}".encode())
        s.connect(f"tcp://{host}:{port}")
        s.send_multipart([b"", json.dumps({"ctrl": "READY", "worker_id": args.worker_id}).encode()])
        return s

    sock = conectar_gt(gt_idx)
    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)

    ultimo_hb_enviado = time.time()
    ultimo_hb_recibido = time.time()
    confirmado = False
    reintentos_mismo_gt = 0
    timeout_actual = GT_FAILOVER_TIMEOUT_S

    print(
        f"[RMC:{args.worker_id}] Conectado a GT {gts[gt_idx][0]}:{gts[gt_idx][1]} "
        f"| replicas={'OFF' if args.sin_replicas else len(bd.replicas)}",
        flush=True,
    )

    try:
        while ejecutando:
            evts = dict(poller.poll(200))
            ahora = time.time()

            if sock in evts:
                while True:
                    try:
                        frames = sock.recv_multipart(flags=zmq.NOBLOCK)
                    except zmq.Again:
                        break
                    ultimo_hb_recibido = time.time()
                    confirmado = True
                    reintentos_mismo_gt = 0
                    timeout_actual = GT_FAILOVER_TIMEOUT_S
                    raw = frames[-1]
                    try:
                        msg = json.loads(raw)
                    except ValueError:
                        continue

                    if msg.get("ctrl") in ("HB_PING", "HB_ACK"):
                        continue

                    try:
                        estado, detalle, seq, origen = procesar(msg, bd)
                    except Exception as exc:
                        estado, detalle, seq, origen = "ERROR", str(exc), 0, "rmc"

                    resp = {
                        "req_id": msg.get("req_id"),
                        "op": msg.get("op"),
                        "estado": estado,
                        "detalle": detalle,
                        "seq": seq,
                        "origen_bd": origen,
                        "worker": args.worker_id,
                    }
                    sock.send_multipart([b"", json.dumps(resp).encode("utf-8")])
                    print(
                        f"[RMC:{args.worker_id}] {msg.get('op')} -> {estado} "
                        f"(bd={origen}, seq={seq})",
                        flush=True,
                    )
                    # Actualizar marca de actividad tras procesar consulta/reserva
                    ultimo_hb_recibido = time.time()

            ahora = time.time()
            if ahora - ultimo_hb_enviado >= RMC_HB_INTERVAL_S:
                sock.send_multipart(
                    [b"", json.dumps({"ctrl": "HB", "worker_id": args.worker_id}).encode()]
                )
                ultimo_hb_enviado = ahora

            if not confirmado and (ahora - ultimo_hb_recibido) >= 0.6:
                poller.unregister(sock)
                sock.close()
                reintentos_mismo_gt += 1
                if len(gts) > 1 and reintentos_mismo_gt > 2:
                    gt_idx = (gt_idx + 1) % len(gts)
                    reintentos_mismo_gt = 0
                    print(
                        f"[RMC:{args.worker_id}] Sin respuesta inicial -> probando GT "
                        f"{gts[gt_idx][0]}:{gts[gt_idx][1]}",
                        flush=True,
                    )
                sock = conectar_gt(gt_idx)
                poller.register(sock, zmq.POLLIN)
                ultimo_hb_recibido = time.time()
                ultimo_hb_enviado = ultimo_hb_recibido
            elif confirmado and (ahora - ultimo_hb_recibido) >= timeout_actual:
                poller.unregister(sock)
                sock.close()
                if len(gts) > 1:
                    gt_idx = (gt_idx + 1) % len(gts)
                    print(
                        f"[RMC:{args.worker_id}] ¡Failover de GT! -> conectado a "
                        f"{gts[gt_idx][0]}:{gts[gt_idx][1]}",
                        flush=True,
                    )
                confirmado = False
                reintentos_mismo_gt = 0
                sock = conectar_gt(gt_idx)
                poller.register(sock, zmq.POLLIN)
                ultimo_hb_recibido = time.time()
                ultimo_hb_enviado = ultimo_hb_recibido
                timeout_actual = GT_FAILOVER_TIMEOUT_S + 1.0
    finally:
        sock.close()
        bd.cerrar()


if __name__ == "__main__":
    main()
