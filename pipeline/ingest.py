"""
ingest.py — Carga y validación de las 3 fuentes raw de CaféNorte.

Decisiones documentadas:
- sales.csv   : se cargan todas las filas; el filtro por tipo_comprobante se aplica en transform.
- inventory   : se extraen las 4 sub-estructuras relevantes del JSON anidado.
- ecommerce   : se lee el parquet directamente; conversión de moneda se hace en transform.
- exchange    : tabla de apoyo, no es fuente primaria de negocio.
"""

import json
import logging
from pathlib import Path

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)
DATA_DIR = Path(__file__).parent.parent / "data"


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def _validate_not_empty(df: pd.DataFrame, name: str) -> None:
    if df.empty:
        raise ValueError(f"[ingest] {name} cargó vacío — revisar ruta o formato.")


# ──────────────────────────────────────────────
# Loaders individuales
# ──────────────────────────────────────────────

def load_sales(path: Path | None = None) -> pd.DataFrame:
    """Carga sales.csv del POS."""
    path = path or DATA_DIR / "sales.csv"
    df = pd.read_csv(path, parse_dates=["fecha_hora"])
    _validate_not_empty(df, "sales")
    logger.info("sales: %d filas cargadas", len(df))
    return df


def load_inventory(path: Path | None = None) -> dict:
    """
    Carga inventory.json y devuelve un dict con 4 DataFrames normalizados:
      - tiendas     : info de cada tienda (ciudad, región, timezone)
      - sku_map     : mapeo sku_pos <-> sku_erp <-> handle Shopify
      - catalogo    : productos con historial de costos (explodido por fecha)
      - snapshots   : stock diario por tienda y SKU
    """
    path = path or DATA_DIR / "inventory.json"
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)

    # 1. Tiendas
    tiendas = pd.DataFrame(raw["tiendas_info"])

    # 2. SKU mappings
    sku_map = pd.DataFrame(raw["sku_mappings"])

    # 3. Catálogo: explodemos cost_history para tener una fila por vigencia
    rows = []
    for prod in raw["catalogo"]["productos"]:
        for ch in prod.get("cost_history", []):
            rows.append({
                "sku_erp":         prod["sku_erp"],
                "nombre":          prod["nombre"],
                "categoria":       prod["categoria"],
                "fecha_vigencia":  pd.to_datetime(ch["fecha_vigencia"]),
                "costo_mxn":       float(ch["costo_mxn"]),
                "proveedor":       ch["proveedor"],
            })
    catalogo = pd.DataFrame(rows)

    # 4. Snapshots de inventario
    snapshots = pd.DataFrame(raw["snapshots"])
    snapshots["fecha"] = pd.to_datetime(snapshots["fecha"])
    snapshots["cantidad_en_stock"] = pd.to_numeric(
        snapshots["cantidad_en_stock"], errors="coerce"
    ).fillna(0).astype(int)

    for name, df in [("tiendas", tiendas), ("sku_map", sku_map),
                     ("catalogo", catalogo), ("snapshots", snapshots)]:
        _validate_not_empty(df, f"inventory.{name}")
        logger.info("inventory.%s: %d filas", name, len(df))

    return {
        "tiendas":   tiendas,
        "sku_map":   sku_map,
        "catalogo":  catalogo,
        "snapshots": snapshots,
    }


def load_ecommerce(path: Path | None = None) -> pd.DataFrame:
    """Carga ecommerce_orders.parquet de Shopify."""
    path = path or DATA_DIR / "ecommerce_orders.parquet"
    df = pd.read_parquet(path)
    df["fecha"] = pd.to_datetime(df["fecha"])
    _validate_not_empty(df, "ecommerce")
    logger.info("ecommerce: %d filas cargadas", len(df))
    return df


def load_exchange_rates(path: Path | None = None) -> pd.DataFrame:
    """Carga tipos de cambio diarios (USD y EUR → MXN)."""
    path = path or DATA_DIR / "exchange_rates.csv"
    df = pd.read_csv(path, parse_dates=["fecha"])
    _validate_not_empty(df, "exchange_rates")
    logger.info("exchange_rates: %d filas cargadas", len(df))
    return df


# ──────────────────────────────────────────────
# Loader unificado (punto de entrada)
# ──────────────────────────────────────────────

def load_all() -> dict:
    """Carga todas las fuentes y devuelve un dict listo para transform."""
    logger.info("=== INGEST: iniciando carga de fuentes ===")
    inv = load_inventory()
    return {
        "sales":          load_sales(),
        "tiendas":        inv["tiendas"],
        "sku_map":        inv["sku_map"],
        "catalogo":       inv["catalogo"],
        "snapshots":      inv["snapshots"],
        "ecommerce":      load_ecommerce(),
        "exchange_rates": load_exchange_rates(),
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    data = load_all()
    for k, v in data.items():
        print(f"{k:20s}: {len(v):>8,} filas")
