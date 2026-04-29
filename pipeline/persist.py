"""
persist.py — Persiste el modelo analítico en un archivo DuckDB.

El archivo .duckdb resultante puede ser consultado directamente con
cualquier cliente DuckDB (CLI, Python, DBeaver, etc.) sin necesidad
de re-correr el pipeline.
"""

import json
import logging
from pathlib import Path

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / "output" / "cafenorte.duckdb"


def _persist_df(con: duckdb.DuckDBPyConnection, tbl_name: str, df: pd.DataFrame) -> None:
    """Persiste un DataFrame como tabla DuckDB. Serializa columnas object complejas a JSON."""
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == object:
            # Detecta columnas con listas/dicts y las serializa a JSON string
            sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            if isinstance(sample, (list, dict)):
                df[col] = df[col].apply(
                    lambda x: json.dumps(x, ensure_ascii=False) if x is not None else None
                )
    con.execute(f"CREATE TABLE {tbl_name} AS SELECT * FROM df")
    count = con.execute(f"SELECT COUNT(*) FROM {tbl_name}").fetchone()[0]
    logger.info("Persistida tabla %s: %d filas", tbl_name, count)


def persist(model: dict, analytics: dict, db_path: Path | None = None) -> Path:
    """
    Guarda todas las tablas del modelo analítico y los resultados
    de las 4 preguntas en un archivo DuckDB.
    """
    db_path = db_path or DB_PATH
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Borra la base anterior para idempotencia
    if db_path.exists():
        db_path.unlink()

    con = duckdb.connect(str(db_path))

    # ── Modelo analítico (tablas base) ──
    tables = {
        "fact_ventas":     model["fact_ventas"],
        "fact_inventario": model["fact_inventario"],
        "dim_tiendas":     model["dim_tiendas"],
        "dim_producto":    model["dim_producto"],      # antes dim_sku
        "dim_catalogo":    model["dim_catalogo"],
    }
    for tbl_name, df in tables.items():
        _persist_df(con, tbl_name, df)

    # ── Resultados de analytics ──
    # Q1, Q2, Q3 son DataFrames simples
    _persist_df(con, "result_q1_rotacion", analytics["q1_rotacion"])
    _persist_df(con, "result_q2_quiebres", analytics["q2_quiebres"])
    _persist_df(con, "result_q3_mom",      analytics["q3_mom"])

    # Q4 devuelve un dict con 3 DataFrames
    q4 = analytics["q4_margen_negativo"]
    _persist_df(con, "result_q4_detalle_por_tienda", q4["detalle_por_tienda"])
    _persist_df(con, "result_q4_resumen_sistemico",  q4["resumen_sistemico"])
    _persist_df(con, "result_q4_resumen_mixto",      q4["resumen_mixto"])

    # ── Índices de uso frecuente ──
    con.execute("CREATE INDEX idx_ventas_fecha       ON fact_ventas(fecha)")
    con.execute("CREATE INDEX idx_ventas_sku         ON fact_ventas(sku_pos)")
    con.execute("CREATE INDEX idx_ventas_product_id  ON fact_ventas(product_id)")
    con.execute("CREATE INDEX idx_ventas_tienda      ON fact_ventas(tienda_id)")
    con.execute("CREATE INDEX idx_inv_fecha          ON fact_inventario(fecha)")
    con.execute("CREATE INDEX idx_inv_tienda_sku     ON fact_inventario(tienda_id, sku_pos)")
    con.execute("CREATE INDEX idx_inv_product_id     ON fact_inventario(product_id)")

    con.close()
    size_mb = db_path.stat().st_size / 1_048_576
    logger.info("Base de datos guardada en %s (%.2f MB)", db_path, size_mb)
    return db_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    from ingest import load_all
    from transform import transform_all
    from analytics import run_all

    raw       = load_all()
    model     = transform_all(raw)
    analytics = run_all(model)
    path      = persist(model, analytics)
    print(f"DB lista: {path}")