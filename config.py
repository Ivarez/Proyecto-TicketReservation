"""Configuración centralizada del Sistema de Reserva de Boletos (SRB - Entrega 2).

Todos los parámetros pueden sobreescribirse mediante variables de entorno con prefijo
``SRB_`` para facilitar el despliegue en 3 máquinas virtuales Linux sin modificar código.
"""
import os

# --- Capa de Negocio: Gestor de Transacciones (GT Primario y Backup) ---
GT_HOST = os.getenv("SRB_GT_HOST", "127.0.0.1")
GT_FRONT_PORT = int(os.getenv("SRB_GT_FRONT_PORT", "5555"))          # Clientes REQ -> GT ROUTER (síncrono)
GT_BACK_PORT = int(os.getenv("SRB_GT_BACK_PORT", "5556"))            # GT ROUTER -> RMC DEALER
GT_ASYNC_PORT = int(os.getenv("SRB_GT_ASYNC_PORT", "5558"))          # Clientes DEALER -> GT ROUTER (cancelación)
GT_CANCEL_PUSH_PORT = int(os.getenv("SRB_GT_CANCEL_PUSH_PORT", "5559"))  # GT PUSH -> Cancelaciones PULL
GT_HB_PORT = int(os.getenv("SRB_GT_HB_PORT", "5561"))                # GT Primario PUB -> GT Backup SUB

GT_BACKUP_HOST = os.getenv("SRB_GT_BACKUP_HOST", GT_HOST)
GT_BACKUP_FRONT_PORT = int(os.getenv("SRB_GT_BACKUP_FRONT_PORT", "5655"))
GT_BACKUP_BACK_PORT = int(os.getenv("SRB_GT_BACKUP_BACK_PORT", "5656"))
GT_BACKUP_ASYNC_PORT = int(os.getenv("SRB_GT_BACKUP_ASYNC_PORT", "5658"))
GT_BACKUP_CANCEL_PUSH_PORT = int(os.getenv("SRB_GT_BACKUP_CANCEL_PUSH_PORT", "5659"))

# --- Capa de Almacenamiento: Persistencia Líder y Réplicas Seguidoras ---
PERSIST_HOST = os.getenv("SRB_PERSIST_HOST", "127.0.0.1")
PERSIST_PORT = int(os.getenv("SRB_PERSIST_PORT", "5557"))            # Escrituras y lecturas en Líder (REP)
PERSIST_REPL_PORT = int(os.getenv("SRB_PERSIST_REPL_PORT", "5565"))  # Canal de replicación PUSH/ACK (ROUTER)
PERSIST_CATCHUP_PORT = int(os.getenv("SRB_PERSIST_CATCHUP_PORT", "5570"))  # Catch-up PULL/REP bajo demanda
PERSIST_HB_PORT = int(os.getenv("SRB_PERSIST_HB_PORT", "5571"))      # Heartbeat PUB del Líder

# Lista separada por comas de endpoints host:puerto de las BD Seguidoras (solo lectura).
# Si SRB_PERSIST_REPLICAS="" se deshabilitan réplicas de lectura y todo va al Líder.
_DEFAULT_REPLICAS = f"{PERSIST_HOST}:5567,{PERSIST_HOST}:5568"
PERSIST_REPLICAS = os.getenv("SRB_PERSIST_REPLICAS", _DEFAULT_REPLICAS)


def parse_replicas(valor: str | None = None) -> list[tuple[str, int]]:
    """Convierte ``host1:puerto1,host2:puerto2`` en una lista de tuplas ``(host, puerto)``."""
    raw = PERSIST_REPLICAS if valor is None else valor
    replicas: list[tuple[str, int]] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        if ":" in item:
            host, puerto = item.rsplit(":", 1)
            replicas.append((host.strip(), int(puerto.strip())))
        else:
            replicas.append((item, PERSIST_PORT))
    return replicas


# --- Parámetros de Replicación y Tolerancia a Fallos ---
MODO_REPLICACION = os.getenv("SRB_MODO_REPL", "semi-sincrono")  # semi-sincrono | sincrono | asincrono | none
REPL_ACK_TIMEOUT_MS = int(os.getenv("SRB_REPL_ACK_TIMEOUT_MS", "200"))

GT_HB_INTERVAL_S = float(os.getenv("SRB_GT_HB_INTERVAL_S", "0.5"))
GT_FAILOVER_TIMEOUT_S = float(os.getenv("SRB_GT_FAILOVER_TIMEOUT_S", "1.5"))
RMC_HB_INTERVAL_S = float(os.getenv("SRB_RMC_HB_INTERVAL_S", "0.5"))
GT_WORKER_EXPIRY_S = float(os.getenv("SRB_GT_WORKER_EXPIRY_S", "2.0"))
GT_TASK_TIMEOUT_S = float(os.getenv("SRB_GT_TASK_TIMEOUT_S", "1.5"))

CLIENT_TIMEOUT_MS = int(os.getenv("SRB_CLIENT_TIMEOUT_MS", "3000"))
CLIENT_MAX_REINTENTOS = int(os.getenv("SRB_CLIENT_MAX_REINTENTOS", "4"))
CLIENT_HOST = os.getenv("SRB_CLIENT_HOST", "")  # Vacío = autodetección de IP LAN hacia GT

# --- Parámetros del Dominio ---
DB_PATH = os.getenv("SRB_DB_PATH", "srb_lider.db")
MES_BASE = os.getenv("SRB_MES", "2026-11")
ASIENTOS_POR_EVENTO = int(os.getenv("SRB_ASIENTOS_POR_EVENTO", "100"))
