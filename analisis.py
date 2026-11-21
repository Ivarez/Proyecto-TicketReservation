"""Calcula métricas de rendimiento (latencia y TPM en GT/cliente) y genera gráficos comparativos.

Soporta dos modos:
1. **Modo simple (Entrega 1)**: ``python analisis.py "resultados_*.csv" --warmup 15``
2. **Modo comparativo Tabla 1**:
   ``python analisis.py --dir resultados_experimentos --warmup 15 --graficos graficos``
"""
import argparse
import csv
import glob
import os
import statistics


def leer_clientes(patron: str, warmup: float) -> list[tuple[float, str, float, str]]:
    filas = []
    for archivo in glob.glob(patron):
        with open(archivo, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                filas.append(
                    (float(r["t_envio"]), r["op"], float(r["latencia_ms"]), r["estado"])
                )
    if not filas:
        return []
    t0 = min(f[0] for f in filas)
    return [f for f in filas if f[0] - t0 >= warmup]


def leer_gt(carpeta: str, warmup: float) -> list[tuple[float, str, float, str]]:
    filas = []
    for archivo in glob.glob(os.path.join(carpeta, "gt_metricas_*.csv")):
        with open(archivo, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                filas.append(
                    (float(r["t_epoch"]), r["op"], float(r["latencia_ms"]), r["estado"])
                )
    if not filas:
        return []
    t0 = min(f[0] for f in filas)
    return [f for f in filas if f[0] - t0 >= warmup]


def resumir_op(filas: list, op: str) -> dict:
    ok = [f[2] for f in filas if f[1] == op and f[3] in ("EXITO", "RECHAZO", "ACEPTADA")]
    mal = sum(1 for f in filas if f[1] == op and f[3] not in ("EXITO", "RECHAZO", "ACEPTADA"))
    if not ok:
        return {"n": 0, "prom": 0.0, "desv": 0.0, "mal": mal}
    sd = statistics.stdev(ok) if len(ok) > 1 else 0.0
    return {"n": len(ok), "prom": statistics.mean(ok), "desv": sd, "mal": mal}


def modo_simple(patron: str, warmup: float) -> None:
    filas = leer_clientes(patron, warmup)
    if not filas:
        raise SystemExit("Sin datos")
    minutos = (max(f[0] for f in filas) - min(f[0] for f in filas)) / 60.0 or 1e-9

    print(f"{'op':14}{'n':>7}{'prom ms':>10}{'desv ms':>10}{'TPM':>9}{'timeouts/errores':>18}")
    for op in ("CONSULTA", "RESERVA", "MODIFICACION", "CANCELACION"):
        r = resumir_op(filas, op)
        if r["n"] or r["mal"]:
            print(
                f"{op:14}{r['n']:>7}{r['prom']:>10.2f}{r['desv']:>10.2f}"
                f"{r['n'] / minutos:>9.1f}{r['mal']:>18}"
            )


def analizar_directorio(dir_exp: str, warmup: float, dir_graficos: str) -> None:
    escenarios = []
    for n_cli in (3, 5, 8):
        for arq in ("base", "escalable"):
            carpeta = os.path.join(dir_exp, f"{arq}_{n_cli}c")
            if not os.path.isdir(carpeta):
                continue
            cli = leer_clientes(os.path.join(carpeta, "resultados_*.csv"), warmup)
            gt = leer_gt(carpeta, warmup)
            if not cli:
                continue

            min_cli = (max(f[0] for f in cli) - min(f[0] for f in cli)) / 60.0 or 1e-9
            min_gt = ((max(f[0] for f in gt) - min(f[0] for f in gt)) / 60.0) if len(gt) > 1 else min_cli

            c_stat = resumir_op(cli, "CONSULTA")
            r_stat = resumir_op(cli, "RESERVA")
            m_stat = resumir_op(cli, "MODIFICACION")

            gt_c = resumir_op(gt, "CONSULTA")["n"] / min_gt if gt else c_stat["n"] / min_cli
            gt_r = resumir_op(gt, "RESERVA")["n"] / min_gt if gt else r_stat["n"] / min_cli
            gt_m = resumir_op(gt, "MODIFICACION")["n"] / min_gt if gt else m_stat["n"] / min_cli

            escenarios.append(
                {
                    "clientes": n_cli,
                    "arq": arq,
                    "c_prom": c_stat["prom"],
                    "c_desv": c_stat["desv"],
                    "r_prom": r_stat["prom"],
                    "r_desv": r_stat["desv"],
                    "m_prom": m_stat["prom"],
                    "m_desv": m_stat["desv"],
                    "tpm_c": gt_c,
                    "tpm_r": gt_r,
                    "tpm_m": gt_m,
                    "errores": c_stat["mal"] + r_stat["mal"] + m_stat["mal"],
                }
            )

    if not escenarios:
        raise SystemExit(f"No se encontraron resultados en {dir_exp}")

    os.makedirs(dir_graficos, exist_ok=True)
    csv_out = os.path.join(dir_graficos, "tabla_resultados.csv")
    with open(csv_out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(escenarios[0].keys()))
        w.writeheader()
        w.writerows(escenarios)

    print("\n=== TABLA DE RESULTADOS (BASE vs ESCALABLE) ===")
    enc = (
        f"{'Cli':>3} {'Arq':10} | {'Cons ms':>9} {'±DE':>7} | {'Res ms':>9} {'±DE':>7} | "
        f"{'TPM Cons':>9} {'TPM Res':>9} {'TPM Mod':>9} {'Err':>4}"
    )
    print(enc)
    print("-" * len(enc))
    for e in escenarios:
        print(
            f"{e['clientes']:>3} {e['arq']:10} | {e['c_prom']:>9.2f} {e['c_desv']:>7.2f} | "
            f"{e['r_prom']:>9.2f} {e['r_desv']:>7.2f} | {e['tpm_c']:>9.1f} {e['tpm_r']:>9.1f} "
            f"{e['tpm_m']:>9.1f} {e['errores']:>4}"
        )

    md_lines = [
        "| Clientes | Arquitectura | Lat. Consulta prom ± DE (ms) | Lat. Reserva prom ± DE (ms) | TPM Consulta (GT) | TPM Reserva (GT) | TPM Modificación (GT) | Errores |",
        "|---:|---|---:|---:|---:|---:|---:|---:|",
    ]
    for e in escenarios:
        md_lines.append(
            f"| {e['clientes']} | {e['arq'].capitalize()} | {e['c_prom']:.2f} ± {e['c_desv']:.2f} | "
            f"{e['r_prom']:.2f} ± {e['r_desv']:.2f} | {e['tpm_c']:.1f} | {e['tpm_r']:.1f} | "
            f"{e['tpm_m']:.1f} | {e['errores']} |"
        )
    with open(os.path.join(dir_graficos, "tabla_resultados.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines) + "\n")

    generar_graficos(escenarios, dir_graficos)


def generar_graficos(escenarios: list[dict], dir_graficos: str) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    clientes = sorted({e["clientes"] for e in escenarios})
    idx = {(e["clientes"], e["arq"]): e for e in escenarios}

    def serie(campo: str, arq: str) -> list[float]:
        return [idx.get((c, arq), {}).get(campo, 0.0) for c in clientes]

    def graficar_par(
        archivo: str,
        titulo: str,
        ylabel: str,
        campo: str,
        campo_err: str | None = None,
    ) -> None:
        x = list(range(len(clientes)))
        ancho = 0.35
        vals_base = serie(campo, "base")
        vals_esc = serie(campo, "escalable")
        err_base = serie(campo_err, "base") if campo_err else None
        err_esc = serie(campo_err, "escalable") if campo_err else None

        fig, ax = plt.subplots(figsize=(7.5, 4.5))
        b1 = ax.bar(
            [p - ancho / 2 for p in x],
            vals_base,
            ancho,
            yerr=err_base,
            capsize=4 if err_base else 0,
            label="Arquitectura Base",
            color="#d95f02",
            alpha=0.88,
        )
        b2 = ax.bar(
            [p + ancho / 2 for p in x],
            vals_esc,
            ancho,
            yerr=err_esc,
            capsize=4 if err_esc else 0,
            label="Arquitectura Escalable",
            color="#1b9e77",
            alpha=0.88,
        )

        ax.set_title(titulo, fontsize=12, fontweight="bold", pad=12)
        ax.set_xlabel("Número de clientes simultáneos", fontsize=10)
        ax.set_ylabel(ylabel, fontsize=10)
        ax.set_xticks(x)
        ax.set_xticklabels([f"{c} clientes" for c in clientes])
        ax.grid(axis="y", linestyle="--", alpha=0.4)

        max_top = 0.0
        for barras, errs in ((b1, err_base), (b2, err_esc)):
            for i, barra in enumerate(barras):
                h = barra.get_height()
                e_val = errs[i] if errs else 0.0
                top = h + e_val
                max_top = max(max_top, top)
                ax.annotate(
                    f"{h:.1f}" if h >= 10 else f"{h:.2f}",
                    xy=(barra.get_x() + barra.get_width() / 2, top),
                    xytext=(0, 4),
                    textcoords="offset points",
                    ha="center",
                    va="bottom",
                    fontsize=8.5,
                    fontweight="bold",
                )

        if max_top > 0:
            ax.set_ylim(0, max_top * 1.20)
        ax.legend(loc="upper left")

        plt.tight_layout()
        plt.savefig(os.path.join(dir_graficos, archivo), dpi=200)
        plt.close(fig)

    graficar_par(
        "grafico_latencia_consulta.png",
        "Tiempo Promedio de Respuesta - Operaciones de Consulta (ms)",
        "Tiempo de respuesta promedio (ms)",
        "c_prom",
        "c_desv",
    )
    graficar_par(
        "grafico_latencia_reserva.png",
        "Tiempo Promedio de Respuesta - Operaciones de Reserva (ms)",
        "Tiempo de respuesta promedio (ms)",
        "r_prom",
        "r_desv",
    )
    graficar_par(
        "grafico_tpm_consulta.png",
        "Throughput - Transacciones de Consulta por Minuto (TPM en GT)",
        "Transacciones por minuto (TPM)",
        "tpm_c",
    )
    graficar_par(
        "grafico_tpm_reserva.png",
        "Throughput - Transacciones de Reserva por Minuto (TPM en GT)",
        "Transacciones por minuto (TPM)",
        "tpm_r",
    )
    print(f"Gráficos generados en: {os.path.abspath(dir_graficos)}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="Análisis de métricas SRB")
    ap.add_argument("patron", nargs="?", default="resultados_*.csv")
    ap.add_argument("--warmup", type=float, default=15.0, help="Segundos iniciales a descartar")
    ap.add_argument("--dir", default=None, help="Directorio de experimentos comparativos")
    ap.add_argument("--graficos", default="graficos", help="Directorio de salida para PNG y tablas")
    args = ap.parse_args()

    if args.dir:
        analizar_directorio(args.dir, args.warmup, args.graficos)
    else:
        modo_simple(args.patron, args.warmup)


if __name__ == "__main__":
    main()
