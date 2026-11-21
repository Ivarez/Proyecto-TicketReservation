"""Servicio de Cancelaciones y Notificaciones Asíncronas del SRB.

Flujo:
1. Recibe órdenes de cancelación por un socket ``PULL`` conectado simultáneamente al
   canal ``PUSH`` del GT Primario y del GT Backup (para sobrevivir al failover del GT).
2. Solicita al **BD Líder** la liberación atómica e idempotente de asientos vía ``REQ/REP``.
3. Envía una notificación directa por ``PUSH`` hacia el ``notif_endpoint`` incluido por
   el proceso cliente en la solicitud original.
"""
import argparse
import collections
import os
import signal
import time
import zmq

from config import (
    GT_BACKUP_CANCEL_PUSH_PORT,
    GT_BACKUP_HOST,
    GT_CANCEL_PUSH_PORT,
    GT_HOST,
    PERSIST_HOST,
    PERSIST_PORT,
)


class CanalPersistenciaLider:
    def __init__(self, ctx: zmq.Context):
        self.ctx = ctx
        self.sock = self._conectar()

    def _conectar(self) -> zmq.Socket:
        s = self.ctx.socket(zmq.REQ)
        s.setsockopt(zmq.RCVTIMEO, 3000)
        s.setsockopt(zmq.SNDTIMEO, 3000)
        s.setsockopt(zmq.LINGER, 0)
        s.connect(f"tcp://{PERSIST_HOST}:{PERSIST_PORT}")
        return s

    def cancelar(self, id_tx: str, id_reserva: int, id_cliente: str | None) -> dict:
        payload = {
            "op": "cancelar_reserva",
            "id_tx": id_tx,
            "id_reserva": id_reserva,
            "id_cliente": id_cliente,
        }
        for intento in range(4):
            try:
                self.sock.send_json(payload)
                return self.sock.recv_json()
            except zmq.Again:
                self.sock.close()
                self.sock = self._conectar()
                if intento < 3:
                    time.sleep(0.15)
        return {"ok": False, "error": "BD_LIDER_NO_RESPONDE"}

    def cerrar(self) -> None:
        self.sock.close()


class EmisorNotificaciones:
    """Cachea sockets PUSH hacia los endpoints de notificación de clientes."""

    def __init__(self, ctx: zmq.Context, max_cache: int = 128):
        self.ctx = ctx
        self.max_cache = max_cache
        self.cache: collections.OrderedDict[str, zmq.Socket] = collections.OrderedDict()

    def enviar(self, endpoint: str, mensaje: dict) -> bool:
        sock = self.cache.get(endpoint)
        if sock is None:
            if len(self.cache) >= self.max_cache:
                _, viejo = self.cache.popitem(last=False)
                viejo.close()
            sock = self.ctx.socket(zmq.PUSH)
            sock.setsockopt(zmq.LINGER, 1500)
            sock.setsockopt(zmq.SNDTIMEO, 1500)
            sock.connect(endpoint)
            self.cache[endpoint] = sock
        else:
            self.cache.move_to_end(endpoint)

        try:
            sock.send_json(mensaje)
            return True
        except zmq.Again:
            return False

    def cerrar(self) -> None:
        for s in self.cache.values():
            s.close()
        self.cache.clear()


def main() -> None:
    ap = argparse.ArgumentParser(description="Servicio de Cancelaciones y Notificaciones SRB")
    ap.add_argument("--worker-id", default=f"cancel-{os.getpid()}")
    ap.add_argument("--solo-primario", action="store_true")
    args = ap.parse_args()

    ctx = zmq.Context.instance()
    pull = ctx.socket(zmq.PULL)
    pull.setsockopt(zmq.LINGER, 0)
    pull.setsockopt(zmq.RCVTIMEO, 500)
    pull.connect(f"tcp://{GT_HOST}:{GT_CANCEL_PUSH_PORT}")
    if not args.solo_primario and (GT_BACKUP_HOST, GT_BACKUP_CANCEL_PUSH_PORT) != (
        GT_HOST,
        GT_CANCEL_PUSH_PORT,
    ):
        pull.connect(f"tcp://{GT_BACKUP_HOST}:{GT_BACKUP_CANCEL_PUSH_PORT}")

    bd = CanalPersistenciaLider(ctx)
    notif = EmisorNotificaciones(ctx)

    ejecutando = True

    def _parar(*_):
        nonlocal ejecutando
        ejecutando = False

    signal.signal(signal.SIGINT, _parar)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _parar)

    print(
        f"[CANCELACIONES:{args.worker_id}] Escuchando PULL en {GT_HOST}:{GT_CANCEL_PUSH_PORT} "
        f"(y backup {GT_BACKUP_HOST}:{GT_BACKUP_CANCEL_PUSH_PORT})",
        flush=True,
    )

    try:
        while ejecutando:
            try:
                msg = pull.recv_json()
            except zmq.Again:
                continue
            except zmq.ZMQError:
                break

            p = msg.get("payload", {})
            id_tx = str(p.get("id_tx", ""))
            id_reserva = p.get("id_reserva")
            id_cliente = p.get("id_cliente")
            notif_ep = p.get("notif_endpoint")

            try:
                rid_int = int(id_reserva)
                r = bd.cancelar(id_tx, rid_int, id_cliente)
            except (ValueError, TypeError):
                r = {"ok": False, "error": "SOLICITUD_INVALIDA"}
            if r.get("ok"):
                estado = "EXITO"
                detalle = r["data"]
            else:
                err = r.get("error", "ERROR_BD")
                estado = (
                    "RECHAZO"
                    if err in ("RESERVA_NO_EXISTE", "RESERVA_YA_CANCELADA", "SOLICITUD_INVALIDA")
                    else "ERROR"
                )
                detalle = err

            evento_notif = {
                "tipo": "NOTIFICACION_CANCELACION",
                "id_tx": id_tx,
                "id_cliente": id_cliente,
                "id_reserva": id_reserva,
                "estado": estado,
                "detalle": detalle,
                "seq": int(r.get("seq", 0)),
                "worker": args.worker_id,
                "ts_notificacion": time.time(),
            }

            ok_envio = False
            if notif_ep:
                ok_envio = notif.enviar(notif_ep, evento_notif)

            print(
                f"[CANCELACIONES:{args.worker_id}] reserva={id_reserva} cliente={id_cliente} "
                f"-> {estado} | notif={'OK' if ok_envio else 'FALLO'} ({notif_ep})",
                flush=True,
            )
    finally:
        pull.close()
        bd.cerrar()
        notif.cerrar()


if __name__ == "__main__":
    main()
