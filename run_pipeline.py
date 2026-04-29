#!/usr/bin/env python3
"""
run_pipeline.py — Punto de entrada del pipeline CaféNorte.

Uso:
    python run_pipeline.py                  # corre todo y guarda DB
    python run_pipeline.py --no-persist     # solo muestra resultados, no guarda
    python run_pipeline.py --output-dir /ruta/personalizada
"""

import argparse
import logging
import sys
import time
from pathlib import Path

# Asegura que el paquete pipeline sea importable desde cualquier CWD
sys.path.insert(0, str(Path(__file__).parent))

from pipeline.ingest import load_all
from pipeline.transform import transform_all
from pipeline.analytics import run_all
from pipeline.persist import persist

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("run_pipeline")


def print_results(results: dict) -> None:
    """Imprime los resultados de las 4 preguntas en consola."""
    sep = "─" * 70

    print(f"\n{sep}")
    print("  RESUMEN EJECUTIVO")
    print(sep)
    kpis = results["resumen"]
    print(f"  Total ventas MXN   : ${kpis['total_ventas_mxn']:>15,.2f}")
    print(f"  Canal físico       : ${kpis['ventas_fisicas_mxn']:>15,.2f}")
    print(f"  Canal e-commerce   : ${kpis['ventas_ecommerce_mxn']:>15,.2f}")
    print(f"  Transacciones      : {kpis['total_transacciones']:>16,}")
    print(f"  Tiendas activas    : {kpis['tiendas_activas']:>16}")
    print(f"  SKUs activos       : {kpis['skus_activos']:>16}")

    print(f"\n{sep}")
    print("  Q1 — Top 10 SKUs por Rotación de Inventario (Oct 2025 – Mar 2026)")
    print(sep)
    cols_q1 = ["sku_pos", "nombre", "categoria", "unidades_fisico",
               "unidades_ecommerce", "stock_promedio",
               "rotacion_fisica", "rotacion_extendida", "pct_ventas_con_inventario"]
    print(results["q1_rotacion"][cols_q1].to_string(index=False))

    print(f"\n{sep}")
    print("  Q2 — Quiebres de Stock >= 3 días (Q1 2026: Ene–Mar)")
    print(sep)
    q2 = results["q2_quiebres"]
    if q2.empty:
        print("  Sin quiebres de 3 días o más en el periodo.")
    else:
        cols_q2 = ["tienda_id", "ciudad", "region", "sku_pos",
                   "inicio_quiebre", "fin_quiebre",
                   "dias_en_quiebre", "ventas_perdidas_estimadas"]
        print(q2[cols_q2].to_string(index=False))
        print(f"\n  Total: {len(q2)} quiebres detectados")

    print(f"\n{sep}")
    print("  Q3 — Crecimiento MoM por Canal (Abr 2025 – Mar 2026)")
    print(sep)
    print(results["q3_mom"].to_string(index=False))

    print(f"\n{sep}")
    print("  Q4 — Productos con Margen Negativo")
    print(sep)
    q4 = results["q4_margen_negativo"]

    print("\n  [ Sistémico — margen negativo en toda la cadena ]")
    sistemico = q4["resumen_sistemico"]
    if sistemico.empty:
        print("  Sin SKUs con margen negativo sistémico.")
    else:
        cols_s = ["sku_pos", "nombre", "tiendas_con_margen_negativo",
                  "margen_total_mxn", "precio_venta_promedio",
                  "costo_unitario_promedio", "brecha_costo_precio"]
        print(sistemico[cols_s].to_string(index=False))

    print("\n  [ Mixto — negativo en algunas tiendas, positivo en total ]")
    mixto = q4["resumen_mixto"]
    if mixto.empty:
        print("  Sin SKUs con margen mixto.")
    else:
        for _, row in mixto.iterrows():
            print(f"\n  {row['sku_pos']} — {row['nombre']} "
                  f"({row['tiendas_con_margen_negativo']} tienda(s) negativa(s) "
                  f"de {row['tiendas_totales']}):")
            for t in row["tiendas_negativas"]:
                print(f"    {t['tienda_id']}  {t['ciudad']:<15}  "
                      f"margen={t['margen_total_mxn']:>10,.0f} MXN  "
                      f"causa={t['posible_causa']}")

    print(f"\n{sep}\n")


def main():
    parser = argparse.ArgumentParser(description="Pipeline CaféNorte")
    parser.add_argument("--no-persist", action="store_true",
                        help="No guardar la base de datos DuckDB")
    parser.add_argument("--output-dir", type=Path, default=None,
                        help="Directorio de salida para la DB")
    args = parser.parse_args()

    t0 = time.time()
    logger.info("Pipeline CaféNorte iniciando")

    # 1. Ingest
    raw = load_all()

    # 2. Transform
    model = transform_all(raw)

    # 3. Analytics
    results = run_all(model)

    # 4. Print
    print_results(results)

    # 5. Persist
    if not args.no_persist:
        db_kwargs = {}
        if args.output_dir:
            db_kwargs["db_path"] = args.output_dir / "cafenorte.duckdb"
        db_path = persist(model, results, **db_kwargs)
        logger.info("Base de datos lista: %s", db_path)

    elapsed = time.time() - t0
    logger.info("Pipeline completado en %.1f segundos", elapsed)


if __name__ == "__main__":
    main()