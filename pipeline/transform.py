"""
transform.py — Normalización, conciliación y modelo analítico de CaféNorte.

Supuestos documentados
──────────────────────
1. VENTAS FÍSICAS: se incluyen tipo_comprobante 'I' (ingreso) y 'E' (egreso).
   Los 'I' suman al revenue; los 'E' restan (monto negativo) para reflejar
   el ingreso neto real: ventas menos devoluciones/notas de crédito.
   P (pagos en parcialidades), N (nómina) y T (traslados) se excluyen por
   no representar transacciones de venta con el cliente.
   SUPUESTO: 'E' sigue nomenclatura CFDI del SAT. Pendiente confirmar con cliente.

2. COSTO VIGENTE: para cada venta se usa el costo más reciente cuya
   fecha_vigencia <= fecha de la venta (LOCF — last observation carried forward).
   Si no existe costo anterior a la venta, se usa el costo más antiguo disponible
   (supuesto conservador: no dejamos filas sin costo).

3. MONEDA: todas las ventas e-commerce se convierten a MXN usando el
   tipo de cambio del día exacto. Para MXN el rate es 1.0 (identidad).
   El archivo exchange_rates cubre Abr 2025 – Mar 2026; el e-commerce
   también comienza en Abr 2025, por lo que no hay gap.

4. QUIEBRES DE STOCK: se define como cantidad_en_stock == 0.
   El inventario (snapshots) cubre Oct 2025 – Mar 2026.
   "Último trimestre" = Ene 2026 – Mar 2026.

5. PRODUCT_ID CANÓNICO: se genera una dimensión de producto unificada que
   asigna un product_id único a cada producto independientemente del sistema
   de origen. Los tres IDs originales (sku_pos, sku_erp, handle) se conservan
   como columnas de trazabilidad.
   - Productos sin sku_pos en el ERP reciben: CN-UNMAPPED-001, CN-UNMAPPED-002...
   - Productos de Shopify sin mapeo reciben:  EC-UNMAPPED-001, EC-UNMAPPED-002...
   Esto garantiza que ningún registro se pierda en la migración.

6. ROTACIÓN DE INVENTARIO: se calcula como unidades_vendidas / stock_promedio
   en el periodo. Se usa el promedio de los snapshots disponibles como
   proxy del stock medio. Periodo = últimos 6 meses desde la fecha máxima
   disponible en ventas (Oct 2025 – Mar 2026, donde ambas fuentes se solapan).
"""

import logging
from pathlib import Path

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# Paso 1: Construir dim_producto (tabla maestra)
# ──────────────────────────────────────────────

def build_dim_producto(
    sku_map: pd.DataFrame,
    catalogo: pd.DataFrame,
    ecommerce: pd.DataFrame,
    sales: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Construye la dimensión de producto canónica con product_id como llave primaria.

    Fuentes de productos:
    - sku_map: puente entre sku_pos (POS), sku_erp (ERP) y handle (Shopify)
    - catalogo: nombre, categoría y costo por sku_erp
    - ecommerce: product_handles que pueden no estar en sku_map
    - sales: sku_pos que pueden existir en ventas sin estar en sku_map

    Casos especiales:
    - sku_erp sin sku_pos → CN-UNMAPPED-XXX (existe en ERP pero no en POS)
    - handle sin sku_pos  → EC-UNMAPPED-XXX (existe en Shopify pero sin mapeo ERP)
    - sku_pos en ventas sin sku_map → se incluye con sku_erp=NULL si no se puede resolver
    """
    # Info base del catálogo: sku_erp → nombre, categoria
    cat_latest = (
        catalogo.sort_values("fecha_vigencia")
        .groupby("sku_erp")[["nombre", "categoria"]]
        .last()
        .reset_index()
    )

    # Base: todos los productos del sku_map
    dim = sku_map.copy()
    dim = dim.merge(cat_latest, on="sku_erp", how="left")

    # sku_pos que aparecen en ventas pero no están en sku_map
    if sales is not None:
        skus_en_ventas = set(sales["sku"].unique())
        skus_en_map    = set(sku_map["sku_pos"].dropna().unique())
        skus_sin_map   = skus_en_ventas - skus_en_map

        if skus_sin_map:
            rows_pos = []
            for sku_pos in sorted(skus_sin_map):
                rows_pos.append({
                    "sku_pos":   sku_pos,
                    "sku_erp":   None,
                    "handle":    None,
                    "nombre":    sku_pos,
                    "categoria": "sin_clasificar",
                })
            dim = pd.concat([dim, pd.DataFrame(rows_pos)], ignore_index=True)
            logger.info(
                "dim_producto: %d sku_pos en ventas sin mapeo ERP agregados",
                len(skus_sin_map)
            )

    # Productos de Shopify sin mapeo en sku_map
    handles_mapeados = set(sku_map["handle"].dropna())
    handles_eco      = set(ecommerce["product_handle"].unique())
    handles_sin_mapeo = handles_eco - handles_mapeados

    if handles_sin_mapeo:
        rows_ec = []
        for i, handle in enumerate(sorted(handles_sin_mapeo), start=1):
            rows_ec.append({
                "sku_pos":   f"EC-UNMAPPED-{i:03d}",
                "sku_erp":   None,
                "handle":    handle,
                "nombre":    handle,
                "categoria": "sin_clasificar",
            })
        dim = pd.concat([dim, pd.DataFrame(rows_ec)], ignore_index=True)

    # Productos del ERP sin sku_pos (en snapshots pero no en sku_map)
    erp_en_map = set(sku_map["sku_erp"].dropna())
    erp_en_cat = set(cat_latest["sku_erp"].dropna())
    erp_sin_pos = erp_en_cat - erp_en_map

    if erp_sin_pos:
        rows_erp = []
        for i, sku_erp in enumerate(sorted(erp_sin_pos), start=1):
            nombre_cat = cat_latest.loc[cat_latest["sku_erp"] == sku_erp, "nombre"]
            cat_val    = cat_latest.loc[cat_latest["sku_erp"] == sku_erp, "categoria"]
            rows_erp.append({
                "sku_pos":   f"CN-UNMAPPED-{i:03d}",
                "sku_erp":   sku_erp,
                "handle":    None,
                "nombre":    nombre_cat.iloc[0] if not nombre_cat.empty else sku_erp,
                "categoria": cat_val.iloc[0] if not cat_val.empty else "sin_clasificar",
            })
        dim = pd.concat([dim, pd.DataFrame(rows_erp)], ignore_index=True)

    # Generar product_id canónico basado en sku_pos
    dim["product_id"] = dim["sku_pos"].apply(
        lambda s: f"PRD-{s}" if pd.notna(s) else None
    )

    dim = dim[["product_id", "sku_pos", "sku_erp", "handle", "nombre", "categoria"]]
    dim = dim.reset_index(drop=True)

    n_mapeados  = (~dim["product_id"].str.contains("UNMAPPED", na=False)).sum()
    n_ec_unmap  = dim["product_id"].str.contains("EC-UNMAPPED", na=False).sum()
    n_erp_unmap = dim["product_id"].str.contains("CN-UNMAPPED", na=False).sum()
    logger.info(
        "dim_producto: %d productos (%d mapeados, %d EC-UNMAPPED, %d CN-UNMAPPED)",
        len(dim), n_mapeados, n_ec_unmap, n_erp_unmap
    )
    return dim


# ──────────────────────────────────────────────
# Paso 2: Normalizar ventas físicas
# ──────────────────────────────────────────────

def transform_sales(sales: pd.DataFrame) -> pd.DataFrame:
    """
    Filtra ventas físicas válidas y estandariza columnas.
    - Incluye tipo_comprobante 'I' (ingreso) y 'E' (egreso)
    - Los 'E' se registran con monto negativo (restan al revenue neto)
    - P, N, T se excluyen (no son transacciones con el cliente)
    - Agrega columna canal = 'fisico'
    - Renombra para schema unificado
    """
    df = sales[sales["tipo_comprobante"].isin(["I", "E"])].copy()
    df = df.rename(columns={
        "venta_id":   "order_id",
        "fecha_hora": "fecha",
        "sku":        "sku_pos",
        "monto":      "monto_original",
    })
    df["canal"]           = "fisico"
    df["moneda_original"] = "MXN"
    # Egresos restan al revenue neto
    df["monto_mxn"] = df.apply(
        lambda r: -r["monto_original"] if r["tipo_comprobante"] == "E" else r["monto_original"],
        axis=1
    )
    df = df[["order_id", "fecha", "tienda_id", "sku_pos",
             "cantidad", "monto_mxn", "canal"]]

    n_ingresos = (sales["tipo_comprobante"] == "I").sum()
    n_egresos  = (sales["tipo_comprobante"] == "E").sum()
    logger.info(
        "sales físicas: %d ingresos + %d egresos = %d filas (de %d raw)",
        n_ingresos, n_egresos, len(df), len(sales)
    )
    return df


# ──────────────────────────────────────────────
# Paso 3: Normalizar e-commerce + conversión FX
# ──────────────────────────────────────────────

def transform_ecommerce(
    ecommerce: pd.DataFrame,
    dim_producto: pd.DataFrame,
    exchange_rates: pd.DataFrame,
) -> pd.DataFrame:
    """
    - Une product_handle → sku_pos via dim_producto (incluye EC-UNMAPPED)
    - Convierte amount → monto_mxn usando FX del día
    - Marca tienda_id = 'ECOMMERCE'
    """
    # Tabla de FX: (fecha, moneda) → rate
    fx = exchange_rates.set_index(["fecha", "currency"])["rate_to_mxn"].to_dict()

    # Mapeo handle → sku_pos desde dim_producto (ahora incluye EC-UNMAPPED)
    handle_to_sku = (
        dim_producto.dropna(subset=["handle"])
        .set_index("handle")["sku_pos"]
        .to_dict()
    )

    df = ecommerce.copy()
    df["sku_pos"]   = df["product_handle"].map(handle_to_sku)
    df["tienda_id"] = "ECOMMERCE"
    df["canal"]     = "ecommerce"

    # Conversión a MXN
    def to_mxn(row):
        if row["currency"] == "MXN":
            return row["amount"]
        key = (row["fecha"].normalize(), row["currency"])
        rate = fx.get(key)
        if rate is None:
            fallback = exchange_rates[
                exchange_rates["currency"] == row["currency"]
            ]["rate_to_mxn"].iloc[0]
            return row["amount"] * fallback
        return row["amount"] * rate

    df["monto_mxn"] = df.apply(to_mxn, axis=1)
    df = df[["order_id", "fecha", "tienda_id", "sku_pos",
             "cantidad", "monto_mxn", "canal"]]

    n_unmapped = df["sku_pos"].str.startswith("EC-UNMAPPED").sum()
    logger.info(
        "ecommerce: %d filas (%d con EC-UNMAPPED, todos incluidos)",
        len(df), n_unmapped
    )
    return df


# ──────────────────────────────────────────────
# Paso 4: Unified fact_ventas
# ──────────────────────────────────────────────

def build_fact_ventas(sales_norm: pd.DataFrame, ecom_norm: pd.DataFrame) -> pd.DataFrame:
    """Concatena ventas físicas y e-commerce en una sola tabla."""
    fact = pd.concat([sales_norm, ecom_norm], ignore_index=True)
    fact = fact.sort_values("fecha").reset_index(drop=True)
    logger.info("fact_ventas: %d filas totales", len(fact))
    return fact


# ──────────────────────────────────────────────
# Paso 5: Agregar product_id y costo vigente
# ──────────────────────────────────────────────

def build_fact_ventas_con_costo(
    fact_ventas: pd.DataFrame,
    catalogo: pd.DataFrame,
    dim_producto: pd.DataFrame,
) -> pd.DataFrame:
    """
    - Agrega product_id desde dim_producto via sku_pos
    - Une con el costo vigente en la fecha de cada venta (LOCF)
    - Calcula margen_mxn y margen_pct
    """
    # sku_pos → product_id y sku_erp
    sku_to_product = dim_producto.set_index("sku_pos")[["product_id", "sku_erp"]].to_dict("index")

    # catalogo ordenado para merge_asof
    cat = catalogo.sort_values("fecha_vigencia")[
        ["sku_erp", "fecha_vigencia", "costo_mxn", "nombre", "categoria"]
    ].copy()

    fv = fact_ventas.copy()
    fv["product_id"] = fv["sku_pos"].map(lambda s: sku_to_product.get(s, {}).get("product_id"))
    fv["sku_erp"]    = fv["sku_pos"].map(lambda s: sku_to_product.get(s, {}).get("sku_erp"))

    result_parts = []
    for sku_erp, group in fv.groupby("sku_erp", dropna=False):
        if pd.isna(sku_erp):
            group = group.copy()
            group["costo_mxn"] = None
            group["nombre"]    = None
            group["categoria"] = None
            result_parts.append(group)
            continue

        cat_sku = cat[cat["sku_erp"] == sku_erp].sort_values("fecha_vigencia")
        if cat_sku.empty:
            group = group.copy()
            group["costo_mxn"] = None
            group["nombre"]    = None
            group["categoria"] = None
            result_parts.append(group)
            continue

        group_sorted = group.sort_values("fecha")
        merged = pd.merge_asof(
            group_sorted,
            cat_sku[["fecha_vigencia", "costo_mxn", "nombre", "categoria"]],
            left_on="fecha",
            right_on="fecha_vigencia",
            direction="backward",
        )
        if merged["costo_mxn"].isna().any():
            merged["costo_mxn"] = merged["costo_mxn"].fillna(cat_sku["costo_mxn"].iloc[0])
            merged["nombre"]    = merged["nombre"].fillna(cat_sku["nombre"].iloc[0])
            merged["categoria"] = merged["categoria"].fillna(cat_sku["categoria"].iloc[0])

        result_parts.append(merged)

    result = pd.concat(result_parts, ignore_index=True)
    result["costo_total_mxn"] = result["costo_mxn"] * result["cantidad"]
    result["margen_mxn"]      = result["monto_mxn"] - result["costo_total_mxn"]
    result["margen_pct"]      = (result["margen_mxn"] / result["monto_mxn"].replace(0, pd.NA)) * 100

    # Orden de columnas legible
    cols = ["order_id", "fecha", "tienda_id", "product_id", "sku_pos", "sku_erp",
            "canal", "cantidad", "monto_mxn", "costo_mxn", "costo_total_mxn",
            "margen_mxn", "margen_pct", "nombre", "categoria"]
    result = result[[c for c in cols if c in result.columns]]

    logger.info(
        "fact_ventas_con_costo: %d filas, %d sin costo (sin sku_erp)",
        len(result), result["costo_mxn"].isna().sum()
    )
    return result


# ──────────────────────────────────────────────
# Paso 6: dim_tiendas
# ──────────────────────────────────────────────

def build_dim_tiendas(tiendas: pd.DataFrame) -> pd.DataFrame:
    """Agrega la tienda virtual ECOMMERCE al catálogo de tiendas."""
    ecom_row = pd.DataFrame([{
        "tienda_id": "ECOMMERCE",
        "ciudad":    "Online",
        "region":    "ecommerce",
        "timezone":  "America/Mexico_City",
    }])
    return pd.concat([tiendas, ecom_row], ignore_index=True)


# ──────────────────────────────────────────────
# Paso 7: fact_inventario con product_id
# ──────────────────────────────────────────────

def build_fact_inventario(
    snapshots: pd.DataFrame,
    dim_producto: pd.DataFrame,
) -> pd.DataFrame:
    """
    Agrega product_id y sku_pos al snapshot de inventario.
    Los sku_erp sin mapeo reciben product_id = PRD-CN-UNMAPPED-XXX.
    Se conservan sku_erp y sku_pos para trazabilidad.
    """
    erp_to_info = (
        dim_producto.dropna(subset=["sku_erp"])
        .set_index("sku_erp")[["product_id", "sku_pos"]]
        .to_dict("index")
    )

    df = snapshots.copy()
    df["product_id"] = df["sku_erp"].map(lambda s: erp_to_info.get(s, {}).get("product_id"))
    df["sku_pos"]    = df["sku_erp"].map(lambda s: erp_to_info.get(s, {}).get("sku_pos"))

    # Orden de columnas
    df = df[["fecha", "tienda_id", "product_id", "sku_pos", "sku_erp", "cantidad_en_stock"]]

    n_unmapped = df["product_id"].isna().sum()
    logger.info(
        "fact_inventario: %d filas (%d sin product_id — pendiente mapeo ERP)",
        len(df), n_unmapped
    )
    return df


# ──────────────────────────────────────────────
# Orquestador
# ──────────────────────────────────────────────

def transform_all(raw: dict) -> dict:
    """Recibe el dict de load_all() y devuelve el modelo analítico completo."""
    logger.info("=== TRANSFORM: iniciando ===")

    # Dimensión de producto primero — es la base de todo
    dim_producto = build_dim_producto(raw["sku_map"], raw["catalogo"], raw["ecommerce"], raw["sales"])

    sales_norm  = transform_sales(raw["sales"])
    ecom_norm   = transform_ecommerce(raw["ecommerce"], dim_producto, raw["exchange_rates"])
    fact_ventas = build_fact_ventas(sales_norm, ecom_norm)
    fact_ventas_costo = build_fact_ventas_con_costo(fact_ventas, raw["catalogo"], dim_producto)
    dim_tiendas = build_dim_tiendas(raw["tiendas"])
    fact_inv    = build_fact_inventario(raw["snapshots"], dim_producto)

    logger.info("=== TRANSFORM: completado ===")
    return {
        "fact_ventas":     fact_ventas_costo,
        "fact_inventario": fact_inv,
        "dim_producto":    dim_producto,
        "dim_tiendas":     dim_tiendas,
        "dim_catalogo":    raw["catalogo"],
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from ingest import load_all
    raw = load_all()
    model = transform_all(raw)
    for k, v in model.items():
        print(f"{k:25s}: {len(v):>8,} filas  |  cols: {list(v.columns)}")