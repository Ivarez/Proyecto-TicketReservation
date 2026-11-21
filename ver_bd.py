"""Utilidad para inspeccionar y comparar bases de datos Líder y Seguidoras en sustentación."""
import argparse
import os
import sqlite3
from config import DB_PATH


def abrir_ro(ruta: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{ruta}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def resumen_bd(conn: sqlite3.Connection) -> dict:
    row = conn.execute(
        "SELECT COALESCE(SUM(asientos_totales),0) AS tot, "
        "COALESCE(SUM(asientos_libres),0) AS lib FROM ocurrencias"
    ).fetchone()
    activas = conn.execute("SELECT COUNT(*) FROM reservas WHERE estado='ACTIVA'").fetchone()[0]
    canceladas = conn.execute("SELECT COUNT(*) FROM reservas WHERE estado='CANCELADA'").fetchone()[0]
    seq_row = conn.execute("SELECT valor FROM meta_estado WHERE clave='ultimo_seq'").fetchone()
    seq = int(seq_row[0]) if seq_row else 0
    return {
        "seq": seq,
        "totales": int(row["tot"]),
        "libres": int(row["lib"]),
        "ocupados": int(row["tot"]) - int(row["lib"]),
        "activas": int(activas),
        "canceladas": int(canceladas),
    }


def snapshot_comparable(conn: sqlite3.Connection) -> tuple[list, list]:
    oc = [
        tuple(r)
        for r in conn.execute(
            "SELECT id_ocurrencia, id_evento, fecha, asientos_totales, asientos_libres "
            "FROM ocurrencias ORDER BY id_ocurrencia"
        ).fetchall()
    ]
    res = [
        tuple(r)
        for r in conn.execute(
            "SELECT id_reserva, id_tx, id_cliente, id_ocurrencia, num_asientos, estado, version "
            "FROM reservas ORDER BY id_reserva"
        ).fetchall()
    ]
    return oc, res


def main() -> None:
    ap = argparse.ArgumentParser(description="Inspector de BD SRB")
    ap.add_argument("--db", default=DB_PATH, help="Ruta de la BD SQLite a inspeccionar")
    ap.add_argument("--evento", default=None, help="Filtrar por un id_evento (ej. E1)")
    ap.add_argument("--reservas", action="store_true", help="Mostrar listado de reservas")
    ap.add_argument(
        "--comparar",
        nargs="+",
        default=None,
        help="Comparar consistencia entre varias BDs (ej. srb_lider.db srb_seg1.db srb_seg2.db)",
    )
    args = ap.parse_args()

    if args.comparar:
        base_path = args.comparar[0]
        if not os.path.exists(base_path):
            raise SystemExit(f"No existe {base_path}")
        c_base = abrir_ro(base_path)
        snap_base = snapshot_comparable(c_base)
        print("=== COMPARACIÓN DE RÉPLICAS ===")
        for ruta in args.comparar:
            if not os.path.exists(ruta):
                print(f"  {ruta:25} NO EXISTE")
                continue
            c = abrir_ro(ruta)
            info = resumen_bd(c)
            igual = snapshot_comparable(c) == snap_base
            estado = "CONSISTENTE" if igual else "DIFERENTE"
            print(
                f"  {ruta:25} seq={info['seq']:<4} libres={info['libres']:<5} "
                f"ocupados={info['ocupados']:<5} activas={info['activas']:<4} "
                f"canceladas={info['canceladas']:<4} [{estado}]"
            )
            c.close()
        c_base.close()
        return

    if not os.path.exists(args.db):
        raise SystemExit(f"No existe la BD: {args.db}")

    conn = abrir_ro(args.db)
    info = resumen_bd(conn)
    print(
        f"=== {args.db} | seq={info['seq']} | totales={info['totales']} "
        f"libres={info['libres']} ocupados={info['ocupados']} "
        f"activas={info['activas']} canceladas={info['canceladas']} ==="
    )

    sql = """SELECT e.id_evento, o.fecha, o.asientos_libres,
                    (o.asientos_totales - o.asientos_libres) AS ocupados,
                    e.ocurrencias_mes
             FROM ocurrencias o JOIN eventos e USING(id_evento)"""
    params: tuple = ()
    if args.evento:
        sql += " WHERE e.id_evento=?"
        params = (args.evento,)
    sql += " ORDER BY CAST(SUBSTR(e.id_evento, 2) AS INT), o.fecha"

    for r in conn.execute(sql, params):
        print(
            f"{r['id_evento']:5} {r['fecha']} (oc={r['ocurrencias_mes']})  "
            f"libres={r['asientos_libres']:3}  ocupados={r['ocupados']:3}"
        )

    if args.reservas:
        print("\n--- Reservas ---")
        q_res = """SELECT r.id_reserva, r.id_cliente, o.id_evento, o.fecha,
                          r.num_asientos, r.estado, r.version, r.id_tx
                   FROM reservas r JOIN ocurrencias o USING(id_ocurrencia)"""
        if args.evento:
            q_res += " WHERE o.id_evento=?"
        q_res += " ORDER BY r.id_reserva"
        for r in conn.execute(q_res, params):
            print(
                f"#{r['id_reserva']:<4} cliente={r['id_cliente']:<8} {r['id_evento']:<4} "
                f"{r['fecha']} asientos={r['num_asientos']:<3} estado={r['estado']:<10} "
                f"v={r['version']} tx={str(r['id_tx'])[:12]}"
            )
    conn.close()


if __name__ == "__main__":
    main()
