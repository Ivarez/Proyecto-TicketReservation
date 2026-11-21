"""Gestor de Transacciones (GT) del SRB: puerta de entrada síncrona y asíncrona.

Patrones ZeroMQ y tolerancia a fallos implementados:
- **Canal Síncrono (``CONSULTA``, ``RESERVA``, ``MODIFICACION``)**:
  - Frontend ``ROUTER`` (`:5555`) <-> Backend ``ROUTER`` (`:5556`) siguiendo el patrón
    *Load-Balancing Broker / Paranoid Pirate* de la guía de ZeroMQ.
  - Los workers RMC (``DEALER``) se registran con ``READY`` y mantienen *heartbeats*
    periódicos. Si un RMC muere en medio de una petición (o expira su *heartbeat*),
    el GT lo expulsa y **reenvía automáticamente la petición en vuelo** (con el mismo
    ``id_tx``) a otro RMC disponible sin que el cliente pierda su transacción.
- **Canal Asíncrono (``CANCELACION``)**:
  - Frontend ``ROUTER`` (`:5558`) recibe solicitudes ``DEALER`` de clientes, responde de
    inmediato ``ACEPTADA`` (no bloqueante) y las empuja por un socket ``PUSH`` (`:5559`)
    hacia las réplicas del servicio ``cancelaciones.py``.
- **Alta Disponibilidad (Primario / Backup Hot Standby)**:
  - El GT Primario publica *heartbeats* por ``PUB`` (`:5561`). El GT Backup escucha por
    ``SUB`` en estado ``STANDBY`` y, si no recibe latidos durante ``GT_FAILOVER_TIMEOUT_S``,
    se promueve automáticamente a ``ACTIVO`` en sus puertos (`5655/5656/5658/5659`).
"""
import argparse
import collections
import csv
import json
import os
import signal
import sys
import time
import uuid
import zmq

from config import (
    GT_ASYNC_PORT,
    GT_BACK_PORT,
    GT_BACKUP_ASYNC_PORT,
    GT_BACKUP_BACK_PORT,
    GT_BACKUP_CANCEL_PUSH_PORT,
    GT_BACKUP_FRONT_PORT,
    GT_CANCEL_PUSH_PORT,
    GT_FAILOVER_TIMEOUT_S,
    GT_FRONT_PORT,
    GT_HB_INTERVAL_S,
    GT_HB_PORT,
    GT_HOST,
    GT_TASK_TIMEOUT_S,
    GT_WORKER_EXPIRY_S,
)

OPS_SINCRONAS = {"CONSULTA", "RESERVA", "MODIFICACION"}
OPS_TODAS = ("CONSULTA", "RESERVA", "MODIFICACION", "CANCELACION")


def responder_front(sock: zmq.Socket, ident: bytes, cuerpo: dict) -> None:
    """Envía respuesta multipart ``[ident, b'', json]`` hacia REQ o DEALER."""
    sock.send_multipart([ident, b"", json.dumps(cuerpo).encode("utf-8")])


def esperar_promocion_backup(
    ctx: zmq.Context, primario_host: str, hb_port: int, stop_check
) -> bool:
    """Escucha heartbeats del GT Primario; retorna True cuando debe promoverse a activo."""
    sub = ctx.socket(zmq.SUB)
    sub.setsockopt(zmq.LINGER, 0)
    sub.setsockopt(zmq.RECONNECT_IVL, 100)
    sub.setsockopt(zmq.RECONNECT_IVL_MAX, 500)
    sub.setsockopt_string(zmq.SUBSCRIBE, "")
    sub.connect(f"tcp://{primario_host}:{hb_port}")

    poller = zmq.Poller()
    poller.register(sub, zmq.POLLIN)

    ultimo_hb = time.time()
    print(
        f"[GT-BACKUP] En STANDBY escuchando heartbeats de {primario_host}:{hb_port} "
        f"(failover tras {GT_FAILOVER_TIMEOUT_S:.1f}s)...",
        flush=True,
    )
    try:
        while not stop_check():
            evts = dict(poller.poll(200))
            ahora = time.time()
            if sub in evts:
                while True:
                    try:
                        _ = sub.recv_json(flags=zmq.NOBLOCK)
                        ultimo_hb = ahora
                    except zmq.Again:
                        break
            if ahora - ultimo_hb >= GT_FAILOVER_TIMEOUT_S:
                print(
                    f"[GT-BACKUP] ¡PRIMARIO CAÍDO (>{GT_FAILOVER_TIMEOUT_S:.1f}s sin heartbeat)! "
                    "Promoviéndose a GT ACTIVO...",
                    flush=True,
                )
                return True
        return False
    except KeyboardInterrupt:
        return False
    finally:
        sub.close()


def abrir_csv_metricas(ruta: str) -> tuple[object, csv.writer]:
    carpeta = os.path.dirname(os.path.abspath(ruta))
    if carpeta:
        os.makedirs(carpeta, exist_ok=True)
    nuevo = not os.path.exists(ruta) or os.path.getsize(ruta) == 0
    f = open(ruta, "a", newline="", encoding="utf-8")
    w = csv.writer(f)
    if nuevo:
        w.writerow(["t_epoch", "rol", "op", "latencia_ms", "estado"])
        f.flush()
    return f, w


def main() -> None:
    ap = argparse.ArgumentParser(description="Gestor de Transacciones SRB")
    ap.add_argument("--rol", choices=["primario", "backup"], default="primario")
    ap.add_argument("--front-port", type=int, default=None)
    ap.add_argument("--back-port", type=int, default=None)
    ap.add_argument("--async-port", type=int, default=None)
    ap.add_argument("--cancel-push-port", type=int, default=None)
    ap.add_argument("--hb-port", type=int, default=GT_HB_PORT)
    ap.add_argument("--primario-host", default=GT_HOST)
    ap.add_argument("--metricas", default=None)
    args = ap.parse_args()

    if args.rol == "primario":
        front_port = args.front_port or GT_FRONT_PORT
        back_port = args.back_port or GT_BACK_PORT
        async_port = args.async_port or GT_ASYNC_PORT
        cancel_push_port = args.cancel_push_port or GT_CANCEL_PUSH_PORT
    else:
        front_port = args.front_port or GT_BACKUP_FRONT_PORT
        back_port = args.back_port or GT_BACKUP_BACK_PORT
        async_port = args.async_port or GT_BACKUP_ASYNC_PORT
        cancel_push_port = args.cancel_push_port or GT_BACKUP_CANCEL_PUSH_PORT

    metricas_path = args.metricas or f"gt_metricas_{args.rol}.csv"
    ctx = zmq.Context.instance()

    ejecutando = True

    def _parar(*_):
        nonlocal ejecutando
        ejecutando = False

    signal.signal(signal.SIGINT, _parar)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _parar)

    cancel_push = ctx.socket(zmq.PUSH)
    cancel_push.setsockopt(zmq.LINGER, 0)
    cancel_push.setsockopt(zmq.SNDHWM, 10000)
    cancel_push.bind(f"tcp://*:{cancel_push_port}")

    if args.rol == "backup":
        if not esperar_promocion_backup(
            ctx, args.primario_host, args.hb_port, lambda: not ejecutando
        ):
            cancel_push.close()
            return

    front = ctx.socket(zmq.ROUTER)
    front.setsockopt(zmq.LINGER, 0)
    front.bind(f"tcp://*:{front_port}")

    back = ctx.socket(zmq.ROUTER)
    back.setsockopt(zmq.LINGER, 0)
    back.bind(f"tcp://*:{back_port}")

    async_front = ctx.socket(zmq.ROUTER)
    async_front.setsockopt(zmq.LINGER, 0)
    async_front.bind(f"tcp://*:{async_port}")

    hb_pub = None
    if args.rol == "primario":
        hb_pub = ctx.socket(zmq.PUB)
        hb_pub.setsockopt(zmq.LINGER, 0)
        hb_pub.bind(f"tcp://*:{args.hb_port}")

    poller = zmq.Poller()
    poller.register(front, zmq.POLLIN)
    poller.register(back, zmq.POLLIN)
    poller.register(async_front, zmq.POLLIN)

    # Estado del broker Paranoid Pirate para workers RMC
    workers_vivos: dict[bytes, float] = {}
    workers_disp: collections.deque[bytes] = collections.deque()
    en_vuelo: dict[str, dict] = {}
    cola_espera: collections.deque[dict] = collections.deque()

    ventana = {op: collections.deque() for op in OPS_TODAS}
    f_met, w_met = abrir_csv_metricas(metricas_path)

    ultimo_hb_pub = 0.0
    ultimo_hb_workers = 0.0
    ultimo_log = time.time()

    print(
        f"[GT-{args.rol.upper()}] ACTIVO | sync=:{front_port} back=:{back_port} "
        f"async=:{async_port} cancel_push=:{cancel_push_port}",
        flush=True,
    )

    def worker_ocupado(w_ident: bytes) -> bool:
        return any(info["worker_ident"] == w_ident for info in en_vuelo.values())

    def encolar_worker_disponible(w_ident: bytes) -> None:
        if w_ident not in workers_disp and not worker_ocupado(w_ident):
            workers_disp.append(w_ident)

    def despachar() -> None:
        while cola_espera and workers_disp:
            w_ident = workers_disp.popleft()
            if w_ident not in workers_vivos:
                continue
            tarea = cola_espera.popleft()
            back.send_multipart([w_ident, b"", json.dumps(tarea["msg"]).encode("utf-8")])
            en_vuelo[tarea["req_id"]] = {
                "client_ident": tarea["client_ident"],
                "msg": tarea["msg"],
                "op": tarea["op"],
                "worker_ident": w_ident,
                "sent_perf": time.perf_counter(),
                "t0_client": tarea["t0_client"],
                "reintentos": tarea["reintentos"],
            }

    def purgar_worker_y_recuperar(w_ident: bytes, motivo: str) -> None:
        workers_vivos.pop(w_ident, None)
        afectados = [rid for rid, info in en_vuelo.items() if info["worker_ident"] == w_ident]
        for rid in afectados:
            info = en_vuelo.pop(rid)
            reintento = info["reintentos"] + 1
            if reintento <= 4:
                print(
                    f"[GT-{args.rol.upper()}] Worker caído ({motivo}) -> reenviando "
                    f"req_id={rid[:8]} op={info['op']} (intento {reintento})",
                    flush=True,
                )
                cola_espera.appendleft(
                    {
                        "req_id": rid,
                        "client_ident": info["client_ident"],
                        "msg": info["msg"],
                        "op": info["op"],
                        "t0_client": info["t0_client"],
                        "reintentos": reintento,
                    }
                )
            else:
                responder_front(
                    front,
                    info["client_ident"],
                    {
                        "req_id": rid,
                        "op": info["op"],
                        "estado": "ERROR",
                        "detalle": "SERVICIO_NO_DISPONIBLE",
                    },
                )

    try:
        while ejecutando:
            evts = dict(poller.poll(150))
            ahora = time.time()
            ahora_perf = time.perf_counter()

            # 1. Mensajes de workers RMC (READY, HB o respuestas de tareas)
            if back in evts:
                while True:
                    try:
                        frames = back.recv_multipart(flags=zmq.NOBLOCK)
                    except zmq.Again:
                        break
                    if len(frames) < 2:
                        continue
                    w_ident, raw = frames[0], frames[-1]
                    try:
                        msg = json.loads(raw)
                    except ValueError:
                        continue

                    workers_vivos[w_ident] = ahora
                    ctrl = msg.get("ctrl")
                    if ctrl == "READY":
                        encolar_worker_disponible(w_ident)
                        back.send_multipart([w_ident, b"", json.dumps({"ctrl": "HB_ACK"}).encode()])
                    elif ctrl == "HB":
                        encolar_worker_disponible(w_ident)
                        back.send_multipart([w_ident, b"", json.dumps({"ctrl": "HB_ACK"}).encode()])
                    else:
                        rid = msg.get("req_id")
                        info = en_vuelo.pop(rid, None)
                        encolar_worker_disponible(w_ident)
                        if info is not None:
                            lat_ms = (ahora_perf - info["t0_client"]) * 1000.0
                            responder_front(front, info["client_ident"], msg)
                            op = info["op"]
                            w_met.writerow(
                                [f"{ahora:.3f}", args.rol, op, f"{lat_ms:.3f}", msg.get("estado")]
                            )
                            ventana[op].append(ahora)

            # 2. Peticiones síncronas de clientes (CONSULTA, RESERVA, MODIFICACION)
            if front in evts:
                while True:
                    try:
                        frames = front.recv_multipart(flags=zmq.NOBLOCK)
                    except zmq.Again:
                        break
                    if len(frames) < 2:
                        continue
                    c_ident, raw = frames[0], frames[-1]
                    try:
                        msg = json.loads(raw)
                        op = msg["op"]
                    except (ValueError, KeyError, TypeError):
                        responder_front(
                            front,
                            c_ident,
                            {"req_id": None, "estado": "ERROR", "detalle": "SOLICITUD_MALFORMADA"},
                        )
                        continue
                    if op not in OPS_SINCRONAS:
                        responder_front(
                            front,
                            c_ident,
                            {"req_id": None, "estado": "ERROR", "detalle": "OPERACION_NO_SOPORTADA"},
                        )
                        continue

                    rid = uuid.uuid4().hex
                    msg["req_id"] = rid
                    cola_espera.append(
                        {
                            "req_id": rid,
                            "client_ident": c_ident,
                            "msg": msg,
                            "op": op,
                            "t0_client": time.perf_counter(),
                            "reintentos": 0,
                        }
                    )

            # 3. Peticiones asíncronas de clientes (CANCELACION)
            if async_front in evts:
                while True:
                    try:
                        frames = async_front.recv_multipart(flags=zmq.NOBLOCK)
                    except zmq.Again:
                        break
                    if len(frames) < 2:
                        continue
                    c_ident, raw = frames[0], frames[-1]
                    t0_c = time.perf_counter()
                    try:
                        msg = json.loads(raw)
                        p = msg.get("payload", {})
                        ok_campos = (
                            msg.get("op") == "CANCELACION"
                            and p.get("id_tx")
                            and p.get("id_reserva") is not None
                            and p.get("notif_endpoint")
                        )
                    except (ValueError, TypeError, AttributeError):
                        ok_campos = False

                    if not ok_campos:
                        responder_front(
                            async_front,
                            c_ident,
                            {
                                "req_id": None,
                                "op": "CANCELACION",
                                "estado": "ERROR",
                                "detalle": "SOLICITUD_MALFORMADA",
                            },
                        )
                        continue

                    rid = uuid.uuid4().hex
                    msg["req_id"] = rid
                    try:
                        cancel_push.send_json(msg, flags=zmq.NOBLOCK)
                        estado_ack = "ACEPTADA"
                    except zmq.Again:
                        estado_ack = "ERROR"

                    responder_front(
                        async_front,
                        c_ident,
                        {
                            "req_id": rid,
                            "op": "CANCELACION",
                            "estado": estado_ack,
                            "id_tx": msg["payload"]["id_tx"],
                        },
                    )
                    lat_ms = (time.perf_counter() - t0_c) * 1000.0
                    w_met.writerow([f"{ahora:.3f}", args.rol, "CANCELACION", f"{lat_ms:.3f}", estado_ack])
                    ventana["CANCELACION"].append(ahora)

            # 4. Detección de workers muertos y tareas estancadas
            muertos = [
                w for w, ts in workers_vivos.items() if ahora - ts > GT_WORKER_EXPIRY_S
            ]
            for w in muertos:
                purgar_worker_y_recuperar(w, "heartbeat expirado")

            vencidas = [
                info["worker_ident"]
                for info in en_vuelo.values()
                if ahora_perf - info["sent_perf"] > GT_TASK_TIMEOUT_S
            ]
            for w in set(vencidas):
                purgar_worker_y_recuperar(w, "timeout de tarea en vuelo")

            # 5. Despachar cola hacia workers disponibles
            despachar()

            # Si no hay ningún worker vivo y hay tareas esperando demasiado (> 2s), responder error
            if not workers_vivos and cola_espera:
                while cola_espera and (ahora_perf - cola_espera[0]["t0_client"]) > 2.0:
                    t_exp = cola_espera.popleft()
                    responder_front(
                        front,
                        t_exp["client_ident"],
                        {
                            "req_id": t_exp["req_id"],
                            "op": t_exp["op"],
                            "estado": "ERROR",
                            "detalle": "SERVICIO_NO_DISPONIBLE",
                        },
                    )

            # 6. Heartbeat del GT Primario hacia Backup y PING hacia RMCs
            if hb_pub is not None and (ahora - ultimo_hb_pub) >= GT_HB_INTERVAL_S:
                hb_pub.send_json({"rol": "primario", "ts": ahora, "workers": len(workers_vivos)})
                ultimo_hb_pub = ahora

            if workers_vivos and (ahora - ultimo_hb_workers) >= GT_HB_INTERVAL_S:
                ping = json.dumps({"ctrl": "HB_PING"}).encode()
                for w in list(workers_vivos.keys()):
                    try:
                        back.send_multipart([w, b"", ping], flags=zmq.NOBLOCK)
                    except zmq.Again:
                        pass
                ultimo_hb_workers = ahora

            # 7. Log periódico de TPM (últimos 60 s)
            if ahora - ultimo_log >= 10.0:
                for dq in ventana.values():
                    while dq and dq[0] < ahora - 60.0:
                        dq.popleft()
                print(
                    f"[GT-{args.rol.upper()}] TPM (60s): consultas={len(ventana['CONSULTA'])} "
                    f"reservas={len(ventana['RESERVA'])} modif={len(ventana['MODIFICACION'])} "
                    f"cancel={len(ventana['CANCELACION'])} | workers={len(workers_vivos)} "
                    f"vuelo={len(en_vuelo)} cola={len(cola_espera)}",
                    flush=True,
                )
                f_met.flush()
                ultimo_log = ahora
    finally:
        f_met.flush()
        f_met.close()
        front.close()
        back.close()
        async_front.close()
        cancel_push.close()
        if hb_pub is not None:
            hb_pub.close()


if __name__ == "__main__":
    main()
