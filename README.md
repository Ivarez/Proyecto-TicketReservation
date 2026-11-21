# SRB — Sistema de Reserva de Boletos

Sistema distribuido basado en servicios sobre **ZeroMQ (PyZMQ)** y **SQLite (WAL)** en arquitectura de tres capas, con operaciones síncronas (**Consulta, Reserva, Modificación**), operación asíncrona con notificaciones directas (**Cancelación**), replicación **Líder Único semi-síncrona** y tolerancia a fallos transparente en **Gestor de Transacciones (GT)** y **RMC**.

---

## 1. Requisitos e Instalación
- Python 3.10+
- Dependencias:
  ```bash
  pip install -r requirements.txt
  ```
  *(Incluye `pyzmq` y `matplotlib`; `sqlite3` viene incluido en Python).*

---

## 2. Arquitectura Final y Puertos

| Capa / VM | Proceso | Rol / Puertos |
|---|---|---|
| **M3 — Almacenamiento** | `persistencia.py --rol lider` | `5557` (REP BD), `5565` (REPL ROUTER), `5570` (Catch-Up REP), `5571` (HB PUB) |
| **M3 — Almacenamiento** | `persistencia.py --rol seguidor` | `5567` (`seg1`, solo lectura), `5568` (`seg2`, solo lectura) |
| **M2 — Negocio** *(ej. `10.43.99.187`)* | `gestor_transacciones.py --rol primario` | `5555` (sync), `5556` (RMC), `5558` (async cancel), `5559` (PUSH cancel), `5561` (HB PUB) |
| **M2 — Negocio** | `gestor_transacciones.py --rol backup` | `5655` (sync), `5656` (RMC), `5658` (async cancel), `5659` (PUSH cancel) |
| **M2 — Negocio** | `supervisor.py` | Vigila y relanza `rmc.py` (4 réplicas) y `cancelaciones.py` (2 réplicas) |
| **M1 — Consumidores** | `cliente_carga.py` | Conecta a M2 (`5555`/`5558` y backup `5655`/`5658`); abre puerto PULL para notificaciones |

### Firewall en VMs Linux (`ufw`)
- **En M3**: abrir `5557`, `5567`, `5568` desde M2 (`5565`, `5570`, `5571` son locales dentro de M3).
- **En M2**: abrir `5555`, `5558`, `5655`, `5658` desde M1.
- **En M1**: si usan firewall activo, usar `--notif-port-base 6000` y abrir `6000:6020/tcp` desde M2.

---

## 3. Ejecución Rápida en 1 Máquina (Prueba Completa)

En terminales separadas dentro de `Proyecto_distribuidos-main/`:

```bash
# Terminal 1 (M3 - BD Líder)
python3 persistencia.py --rol lider --reset

# Terminal 2 y 3 (M3 - BD Seguidoras)
python3 persistencia.py --rol seguidor --id seg1 --reset --puerto 5567 --db srb_seg1.db
python3 persistencia.py --rol seguidor --id seg2 --reset --puerto 5568 --db srb_seg2.db

# Terminal 4 y 5 (M2 - GT Primario y Backup Hot Standby)
python3 gestor_transacciones.py --rol primario
python3 gestor_transacciones.py --rol backup

# Terminal 6 (M2 - Supervisor con 4 RMCs y 2 Cancelaciones)
python3 supervisor.py --rmc 4 --cancel 2

# Terminal 7 (M1 - Caso de prueba determinístico y verificación de BD)
python3 cliente_carga.py --script casos_prueba/caso_demo_profesor.json
python3 ver_bd.py --comparar srb_lider.db srb_seg1.db srb_seg2.db --reservas
```

---

## 4. Despliegue en 3 Máquinas Virtuales Linux

Dar permisos de ejecución una sola vez en las 3 VMs:
```bash
chmod +x scripts_vm/*.sh
```

### Arquitectura Escalable (Final)
1. **En M3 (Almacenamiento)**:
   ```bash
   ./scripts_vm/iniciar_m3_escalable.sh
   ```
   *(Para cargar parámetros de sustentación, ej.: `./scripts_vm/iniciar_m3_escalable.sh --eventos 20 --dobles 8 --asientos 50` o `--seed-file mi_caso.json`)*.
2. **En M2 (Negocio, ej. IP `10.43.99.187`)**:
   ```bash
   ./scripts_vm/iniciar_m2_escalable.sh <IP_M3>
   ```
3. **En M1 (Consumidores)** — apuntando a M2 (ej. `10.43.99.187`):
   ```bash
   # Caso de prueba del profesor:
   SRB_GT_HOST=10.43.99.187 python3 cliente_carga.py --script casos_prueba/caso_demo_profesor.json

   # Escenarios de Tabla 1 (3, 5 u 8 clientes durante 150 s):
   ./scripts_vm/iniciar_m1_escenario.sh 10.43.99.187 3 escalable 150
   ./scripts_vm/iniciar_m1_escenario.sh 10.43.99.187 5 escalable 150
   ./scripts_vm/iniciar_m1_escenario.sh 10.43.99.187 8 escalable 150
   ```

### Arquitectura Base (Para Comparativa de Tabla 1)
1. **En M3**: `./scripts_vm/iniciar_m3_base.sh`
2. **En M2**: `./scripts_vm/iniciar_m2_base.sh <IP_M3>`
3. **En M1**: `./scripts_vm/iniciar_m1_escenario.sh 10.43.99.187 <3|5|8> base 150`

---

## 5. Demostración de Tolerancia a Fallos en Sustentación

Mientras hay clientes ejecutándose en M1 (ej. `SRB_GT_HOST=10.43.99.187 python3 cliente_carga.py --cliente-id DEMO --duracion 60 --rpm 120 --pct-consulta 40 --pct-reserva 30 --pct-modificacion 20 --pct-cancelacion 10`):

- **Fallo de RMC**: en M2 buscar un PID con `pgrep -af rmc.py` y ejecutar `kill -9 <PID_RMC>`.
  - El GT detecta la caída y reenvía la petición en vuelo a otro RMC sano sin error en el cliente.
  - `supervisor.py` detecta el proceso caído y lanza un reemplazo en `< 0.5 s`.
- **Fallo de GT Primario**: en M2 buscar el PID con `pgrep -af "gestor_transacciones.py --rol primario"` y ejecutar `kill -9 <PID_GT1>`.
  - `gestor_transacciones.py --rol backup` detecta la falta de *heartbeat* (`1.5 s`) y se promueve a **ACTIVO**.
  - Los RMCs reconectan al Backup y el cliente aplica *Lazy Pirate* reintentando con el mismo `id_tx` (cero duplicados, cero pérdidas).

---

## 6. Pruebas Automatizadas, Experimentos y Gráficos

```bash
# Suite de 5 pruebas end-to-end (funcional, concurrencia, replicación, kill RMC, kill GT)
python3 test_sistema.py

# Corrida completa de los 6 escenarios de Tabla 1 (en una máquina)
python3 ejecutar_experimentos.py --duracion 150 --rpm 0 --pausa-ms 5 --salida-dir resultados_experimentos

# Consolidar tabla de métricas y generar los 4 gráficos PNG
python3 analisis.py --dir resultados_experimentos --warmup 15 --graficos graficos
```

---

## 7. Archivos del Proyecto
- `config.py` · `requirements.txt`
- `persistencia.py` · `ver_bd.py`
- `gestor_transacciones.py` · `rmc.py` · `cancelaciones.py` · `supervisor.py`
- `cliente_carga.py` · `casos_prueba/caso_demo_profesor.json`
- `ejecutar_experimentos.py` · `analisis.py` · `test_sistema.py`
- `scripts_vm/iniciar_m3_base.sh` · `iniciar_m3_escalable.sh` · `iniciar_m2_base.sh` · `iniciar_m2_escalable.sh` · `iniciar_m1_escenario.sh`
