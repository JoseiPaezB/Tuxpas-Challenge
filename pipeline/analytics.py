"""
analytics.py — Las 4 preguntas de negocio de CaféNorte respondidas con DuckDB.

Cada función recibe el modelo analítico (dict de DataFrames) y devuelve
un DataFrame con el resultado listo para mostrar o exportar.

Supuestos adicionales por pregunta (ver también transform.py):
──────────────────────────────────────────────────────────────
Q1 (Rotación): periodo = últimos 6 meses con solapamiento entre ventas
   e inventario = Oct 2025 – Mar 2026. Rotación = unidades_vendidas / stock_promedio.
   Se incluyen ambos canales (físico + ecommerce) porque el cliente solicitó
   visibilidad de TODAS las tiendas y se asume inventario compartido.
   SKUs EC-UNMAPPED se excluyen por no tener conciliación con el ERP.

Q2 (Quiebres): "último trimestre" = Ene–Mar 2026 (Q1 2026).
   Se detectan rachas consecutivas de días con stock == 0 por tienda+SKU.
   Solo se reportan rachas > 3 días.

Q3 (MoM): "último año" = Abr 2025 – Mar 2026 (12 meses completos con
   solapamiento de ambos canales). Se normaliza en MXN.

Q4 (Margen negativo): monto_mxn < costo_total_mxn. Solo filas con costo
   conocido. Se agrega por SKU+tienda para detectar patrones, no casos
   aislados de un ticket raro.
"""

import logging

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


def _duck(model: dict) -> duckdb.DuckDBPyConnection:
    """Crea una conexión DuckDB en memoria y registra todas las tablas del modelo."""
    con = duckdb.connect(":memory:")
    for name, df in model.items():
        con.register(name, df)
    return con


# ──────────────────────────────────────────────
# Q1: Top 10 SKUs por rotación de inventario
#     (últimos 6 meses)
# ──────────────────────────────────────────────

def q1_top10_rotacion(model: dict) -> pd.DataFrame:
    """
    Tres métricas de rotación complementarias por SKU.
    Periodo: Oct 2025 – Mar 2026 (solapamiento inventario + ventas)

    rotacion_fisica      = ventas_POS / stock_promedio
                           → rotación del inventario controlado por el ERP
    rotacion_extendida   = (ventas_POS + ventas_ecom) / stock_promedio
                           → impacto real de todos los canales sobre el mismo inventario
    pct_ventas_con_inv   = ventas_POS / ventas_totales
                           → qué tan dependiente es el producto del canal físico
                           → bajo (< 50%) = producto principalmente online
                           → alto (> 80%) = producto principalmente en tienda

    SKUs EC-UNMAPPED se excluyen por no tener conciliación con el ERP.
    Ranking principal: rotacion_extendida DESC (visión completa del cliente).
    """
    con = _duck(model)
    result = con.execute("""
        WITH periodo AS (
            SELECT DATE '2025-10-01' AS inicio, DATE '2026-03-31' AS fin
        ),

        ventas_6m AS (
            SELECT
                sku_pos,
                nombre,
                categoria,
                SUM(cantidad)                                        AS unidades_totales,
                SUM(CASE WHEN canal = 'fisico'    THEN cantidad ELSE 0 END) AS unidades_fisico,
                SUM(CASE WHEN canal = 'ecommerce' THEN cantidad ELSE 0 END) AS unidades_ecommerce
            FROM fact_ventas
            CROSS JOIN periodo
            WHERE sku_pos IS NOT NULL
              AND sku_pos NOT LIKE 'EC-UNMAPPED%'
              AND CAST(fecha AS DATE) BETWEEN inicio AND fin
            GROUP BY sku_pos, nombre, categoria
        ),

        stock_prom AS (
            SELECT
                sku_pos,
                ROUND(AVG(cantidad_en_stock), 2) AS stock_promedio
            FROM fact_inventario
            CROSS JOIN periodo
            WHERE sku_pos IS NOT NULL
              AND fecha BETWEEN inicio AND fin
            GROUP BY sku_pos
        ),

        rotacion AS (
            SELECT
                v.sku_pos,
                v.nombre,
                v.categoria,
                v.unidades_fisico,
                v.unidades_ecommerce,
                v.unidades_totales,
                COALESCE(s.stock_promedio, 0) AS stock_promedio,

                -- Rotación física: solo POS vs inventario ERP
                CASE
                    WHEN COALESCE(s.stock_promedio, 0) = 0 THEN NULL
                    ELSE ROUND(v.unidades_fisico / s.stock_promedio, 2)
                END AS rotacion_fisica,

                -- Rotación extendida: todos los canales vs mismo inventario
                CASE
                    WHEN COALESCE(s.stock_promedio, 0) = 0 THEN NULL
                    ELSE ROUND(v.unidades_totales / s.stock_promedio, 2)
                END AS rotacion_extendida,

                -- % de ventas respaldadas por inventario físico
                CASE
                    WHEN v.unidades_totales = 0 THEN NULL
                    ELSE ROUND(v.unidades_fisico * 100.0 / v.unidades_totales, 1)
                END AS pct_ventas_con_inventario
            FROM ventas_6m v
            LEFT JOIN stock_prom s USING (sku_pos)
        )

        SELECT *
        FROM rotacion
        WHERE rotacion_extendida IS NOT NULL
        ORDER BY rotacion_extendida DESC
        LIMIT 10
    """).df()

    logger.info("Q1 completada: %d filas", len(result))
    return result


# ──────────────────────────────────────────────
# Q2: Tiendas con quiebres de stock > 3 días
#     (último trimestre: Ene–Mar 2026)
# ──────────────────────────────────────────────

def q2_quiebres_stock(model: dict) -> pd.DataFrame:
    """
    Detecta rachas consecutivas de días con stock == 0 por tienda+SKU en Q1 2026.
    Reporta rachas >= 3 días (inclusive).

    ventas_perdidas_estimadas = avg_unidades_diarias_14d_previos * dias_en_quiebre
    Se calcula con ventas físicas de los 14 días previos al inicio del quiebre.
    Si no hay ventas previas en ese periodo, se reporta como 0.
    """
    con = _duck(model)
    result = con.execute("""
        WITH inv_q1 AS (
            SELECT
                i.fecha,
                i.tienda_id,
                i.sku_pos,
                i.cantidad_en_stock,
                (cantidad_en_stock = 0) AS en_quiebre
            FROM fact_inventario i
            WHERE i.fecha BETWEEN DATE '2026-01-01' AND DATE '2026-03-31'
              AND i.sku_pos IS NOT NULL
        ),

        -- Island-gap: grupo por racha consecutiva
        with_grp AS (
            SELECT *,
                SUM(CASE WHEN en_quiebre THEN 0 ELSE 1 END)
                    OVER (PARTITION BY tienda_id, sku_pos ORDER BY fecha) AS grp
            FROM inv_q1
        ),

        -- Rachas >= 3 días con stock = 0
        rachas AS (
            SELECT
                tienda_id,
                sku_pos,
                MIN(fecha) AS inicio_quiebre,
                MAX(fecha) AS fin_quiebre,
                COUNT(*)   AS dias_en_quiebre
            FROM with_grp
            WHERE en_quiebre
            GROUP BY tienda_id, sku_pos, grp
            HAVING COUNT(*) >= 3
        ),

        -- Promedio de ventas diarias en los 14 días previos al quiebre
        ventas_previas AS (
            SELECT
                r.tienda_id,
                r.sku_pos,
                r.inicio_quiebre,
                COALESCE(
                    SUM(fv.cantidad) / 14.0,
                    0
                ) AS avg_unidades_diarias_14d
            FROM rachas r
            LEFT JOIN fact_ventas fv
                ON  fv.tienda_id = r.tienda_id
                AND fv.sku_pos   = r.sku_pos
                AND fv.canal     = 'fisico'
                AND CAST(fv.fecha AS DATE) BETWEEN (r.inicio_quiebre - INTERVAL '14 days')
                                               AND (r.inicio_quiebre - INTERVAL '1 day')
            GROUP BY r.tienda_id, r.sku_pos, r.inicio_quiebre
        )

        SELECT
            r.tienda_id,
            t.ciudad,
            t.region,
            r.sku_pos,
            r.inicio_quiebre,
            r.fin_quiebre,
            r.dias_en_quiebre,
            ROUND(vp.avg_unidades_diarias_14d, 2)                          AS avg_unidades_diarias_14d,
            ROUND(vp.avg_unidades_diarias_14d * r.dias_en_quiebre, 1)      AS ventas_perdidas_estimadas
        FROM rachas r
        LEFT JOIN dim_tiendas t  USING (tienda_id)
        LEFT JOIN ventas_previas vp
            ON  vp.tienda_id      = r.tienda_id
            AND vp.sku_pos        = r.sku_pos
            AND vp.inicio_quiebre = r.inicio_quiebre
        ORDER BY ventas_perdidas_estimadas DESC, r.dias_en_quiebre DESC
    """).df()

    logger.info("Q2 completada: %d quiebres >= 3 días", len(result))
    return result


# ──────────────────────────────────────────────
# Q3: Crecimiento MoM por canal
#     (último año: Abr 2025 – Mar 2026)
# ──────────────────────────────────────────────

def q3_mom_por_canal(model: dict) -> pd.DataFrame:
    """
    Ventas mensuales en MXN por canal (fisico / ecommerce), con:
    - venta_diaria_promedio: normaliza por días del mes para comparaciones justas
    - variacion_pct: basada en venta_diaria_promedio, no en totales (evita sesgo por días)
    - share_ecommerce: % del revenue total que representa Shopify cada mes
    """
    con = _duck(model)
    result = con.execute("""
        WITH ventas_mes AS (
            SELECT
                DATE_TRUNC('month', fecha)                    AS mes,
                canal,
                ROUND(SUM(monto_mxn), 2)                     AS venta_mxn,
                DAY(LAST_DAY(DATE_TRUNC('month', fecha)))     AS dias_en_mes,
                ROUND(SUM(monto_mxn) /
                    DAY(LAST_DAY(DATE_TRUNC('month', fecha))), 2) AS venta_diaria_promedio
            FROM fact_ventas
            WHERE fecha BETWEEN DATE '2025-04-01' AND DATE '2026-03-31'
            GROUP BY 1, 2
        ),

        -- MoM basado en venta diaria promedio para comparación justa entre meses
        con_lag AS (
            SELECT
                mes,
                canal,
                dias_en_mes,
                venta_mxn,
                venta_diaria_promedio,
                LAG(venta_diaria_promedio) OVER (PARTITION BY canal ORDER BY mes) AS venta_diaria_mes_anterior,
                ROUND(
                    (venta_diaria_promedio
                        - LAG(venta_diaria_promedio) OVER (PARTITION BY canal ORDER BY mes))
                    / NULLIF(LAG(venta_diaria_promedio) OVER (PARTITION BY canal ORDER BY mes), 0)
                    * 100, 2
                ) AS variacion_pct_diaria
            FROM ventas_mes
        ),

        -- Pivot para calcular share: ambos canales en la misma fila por mes
        pivot_mes AS (
            SELECT
                mes,
                MAX(CASE WHEN canal = 'fisico'    THEN venta_mxn END) AS venta_fisico,
                MAX(CASE WHEN canal = 'ecommerce' THEN venta_mxn END) AS venta_ecommerce
            FROM con_lag
            GROUP BY mes
        )

        SELECT
            STRFTIME(c.mes, '%Y-%m')                          AS mes,
            c.canal,
            c.dias_en_mes,
            c.venta_mxn,
            c.venta_diaria_promedio,
            c.variacion_pct_diaria                            AS variacion_pct_mom,
            ROUND(
                COALESCE(p.venta_ecommerce, 0) /
                NULLIF(COALESCE(p.venta_fisico, 0) + COALESCE(p.venta_ecommerce, 0), 0)
                * 100, 2
            )                                                 AS share_ecommerce_pct
        FROM con_lag c
        LEFT JOIN pivot_mes p USING (mes)
        ORDER BY c.mes, c.canal
    """).df()

    logger.info("Q3 completada: %d filas", len(result))
    return result


# ──────────────────────────────────────────────
# Q4: Productos con margen negativo y tiendas
# ──────────────────────────────────────────────

def q4_margen_negativo(model: dict) -> pd.DataFrame:
    """
    Productos donde el monto_mxn < costo_total_mxn (margen negativo).
    Solo filas con costo conocido.

    Devuelve un dict con TRES DataFrames:
    - detalle_por_tienda  : una fila por SKU+tienda con margen negativo.
                            Incluye posible_causa inferida de los datos:
                            · 'precio_regional_bajo'     → precio promedio de la tienda
                              es >10% menor al precio nacional del SKU
                            · 'promocion_mal_configurada'→ pocas transacciones (<=5) y
                              pocas unidades (<=10) — margen negativo puntual
                            · 'merma_operativa'          → muchas transacciones sostenidas
                              con margen negativo (>5 transacciones)
    - resumen_sistemico   : SKUs cuyo margen agregado total en la cadena es negativo
    - resumen_mixto       : SKUs con margen negativo en algunas tiendas pero positivo
                            en el total, con posible_causa por cada tienda afectada
    """
    con = _duck(model)

    # ── Precio nacional promedio por SKU (referencia para detectar precio regional bajo) ──
    precio_nacional = con.execute("""
        SELECT
            sku_pos,
            ROUND(AVG(monto_mxn / NULLIF(cantidad, 0)), 2) AS precio_nacional_promedio
        FROM fact_ventas
        WHERE costo_mxn IS NOT NULL
          AND cantidad > 0
        GROUP BY sku_pos
    """).df()
    con.register("precio_nacional", precio_nacional)

    # ── Vista detalle: SKU + tienda con posible_causa ──
    detalle = con.execute("""
        WITH ventas_con_costo AS (
            SELECT *
            FROM fact_ventas
            WHERE costo_mxn IS NOT NULL
              AND margen_mxn IS NOT NULL
        ),

        por_sku_tienda AS (
            SELECT
                sku_pos,
                nombre,
                categoria,
                tienda_id,
                canal,
                COUNT(*)                                               AS num_transacciones,
                SUM(cantidad)                                          AS unidades,
                ROUND(SUM(monto_mxn), 2)                               AS ingreso_total_mxn,
                ROUND(SUM(costo_total_mxn), 2)                         AS costo_total_mxn,
                ROUND(SUM(margen_mxn), 2)                              AS margen_total_mxn,
                ROUND(AVG(monto_mxn / NULLIF(cantidad, 0)), 2)         AS precio_venta_promedio,
                ROUND(AVG(costo_mxn), 2)                               AS costo_unitario_promedio,
                ROUND(AVG(margen_pct), 2)                              AS margen_prom_pct
            FROM ventas_con_costo
            GROUP BY sku_pos, nombre, categoria, tienda_id, canal
            HAVING SUM(margen_mxn) < 0
        ),

        con_causa AS (
            SELECT
                p.*,
                pn.precio_nacional_promedio,
                CASE
                    -- Precio regional bajo: precio local >10% menor al nacional
                    WHEN p.precio_venta_promedio < pn.precio_nacional_promedio * 0.90
                        THEN 'precio_regional_bajo'
                    -- Promoción mal configurada: pocas transacciones y pocas unidades
                    WHEN p.num_transacciones <= 5 AND p.unidades <= 10
                        THEN 'promocion_mal_configurada'
                    -- Merma / problema operativo: muchas transacciones sostenidas
                    ELSE 'merma_operativa'
                END AS posible_causa
            FROM por_sku_tienda p
            LEFT JOIN precio_nacional pn USING (sku_pos)
        )

        SELECT
            c.*,
            t.ciudad,
            t.region
        FROM con_causa c
        LEFT JOIN dim_tiendas t USING (tienda_id)
        ORDER BY margen_total_mxn ASC
    """).df()

    # ── Vista resumen agregada por SKU ──
    resumen_base = con.execute("""
        WITH ventas_con_costo AS (
            SELECT *
            FROM fact_ventas
            WHERE costo_mxn IS NOT NULL
              AND margen_mxn IS NOT NULL
        ),

        skus_con_alguna_tienda_negativa AS (
            SELECT DISTINCT sku_pos
            FROM ventas_con_costo
            GROUP BY sku_pos, tienda_id
            HAVING SUM(margen_mxn) < 0
        ),

        resumen_sku AS (
            SELECT
                v.sku_pos,
                v.nombre,
                v.categoria,
                COUNT(DISTINCT v.tienda_id)                            AS tiendas_totales,
                COUNT(DISTINCT CASE
                    WHEN v.tienda_id IN (
                        SELECT tienda_id FROM ventas_con_costo
                        WHERE sku_pos = v.sku_pos
                        GROUP BY tienda_id
                        HAVING SUM(margen_mxn) < 0
                    ) THEN v.tienda_id END)                            AS tiendas_con_margen_negativo,
                COUNT(*)                                               AS num_transacciones,
                SUM(v.cantidad)                                        AS unidades_totales,
                ROUND(SUM(v.monto_mxn), 2)                             AS ingreso_total_mxn,
                ROUND(SUM(v.costo_total_mxn), 2)                       AS costo_total_mxn,
                ROUND(SUM(v.margen_mxn), 2)                            AS margen_total_mxn,
                ROUND(AVG(v.monto_mxn / NULLIF(v.cantidad, 0)), 2)     AS precio_venta_promedio,
                ROUND(AVG(v.costo_mxn), 2)                             AS costo_unitario_promedio,
                ROUND(
                    AVG(v.costo_mxn) - AVG(v.monto_mxn / NULLIF(v.cantidad, 0)), 2
                )                                                      AS brecha_costo_precio
            FROM ventas_con_costo v
            INNER JOIN skus_con_alguna_tienda_negativa s USING (sku_pos)
            GROUP BY v.sku_pos, v.nombre, v.categoria
        )

        SELECT *
        FROM resumen_sku
        ORDER BY margen_total_mxn ASC
    """).df()

    # Separar sistémico vs mixto
    sistemico = resumen_base[resumen_base["margen_total_mxn"] < 0].copy()
    mixto     = resumen_base[resumen_base["margen_total_mxn"] >= 0].copy()

    # Para resumen_mixto: agregar las tiendas afectadas con su posible_causa
    if not mixto.empty:
        detalle_mixto = detalle[detalle["sku_pos"].isin(mixto["sku_pos"])][
            ["sku_pos", "tienda_id", "ciudad", "region",
             "margen_total_mxn", "precio_venta_promedio",
             "precio_nacional_promedio", "posible_causa"]
        ].copy()
        mixto = mixto.merge(
            detalle_mixto.groupby("sku_pos").apply(
                lambda g: g[["tienda_id", "ciudad", "posible_causa",
                              "margen_total_mxn"]].to_dict("records"),
                include_groups=False
            ).rename("tiendas_negativas"),
            on="sku_pos",
            how="left"
        )

    logger.info(
        "Q4 completada: %d filas detalle | %d SKUs sistémicos | %d SKUs mixtos",
        len(detalle), len(sistemico), len(mixto)
    )
    return {
        "detalle_por_tienda": detalle,
        "resumen_sistemico":  sistemico,
        "resumen_mixto":      mixto,
    }


# ──────────────────────────────────────────────
# Resumen ejecutivo rápido
# ──────────────────────────────────────────────

def resumen_ejecutivo(model: dict) -> dict:
    """KPIs de alto nivel para el README."""
    con = _duck(model)
    kpis = {}

    kpis["total_ventas_mxn"] = con.execute(
        "SELECT ROUND(SUM(monto_mxn), 2) FROM fact_ventas"
    ).fetchone()[0]

    kpis["ventas_fisicas_mxn"] = con.execute(
        "SELECT ROUND(SUM(monto_mxn), 2) FROM fact_ventas WHERE canal='fisico'"
    ).fetchone()[0]

    kpis["ventas_ecommerce_mxn"] = con.execute(
        "SELECT ROUND(SUM(monto_mxn), 2) FROM fact_ventas WHERE canal='ecommerce'"
    ).fetchone()[0]

    kpis["total_transacciones"] = con.execute(
        "SELECT COUNT(*) FROM fact_ventas"
    ).fetchone()[0]

    kpis["tiendas_activas"] = con.execute(
        "SELECT COUNT(DISTINCT tienda_id) FROM fact_ventas WHERE canal='fisico'"
    ).fetchone()[0]

    kpis["skus_activos"] = con.execute(
        "SELECT COUNT(DISTINCT sku_pos) FROM fact_ventas WHERE sku_pos IS NOT NULL"
    ).fetchone()[0]

    return kpis


# ──────────────────────────────────────────────
# Runner
# ──────────────────────────────────────────────

def run_all(model: dict) -> dict:
    logger.info("=== ANALYTICS: corriendo las 4 preguntas ===")
    return {
        "q1_rotacion":        q1_top10_rotacion(model),
        "q2_quiebres":        q2_quiebres_stock(model),
        "q3_mom":             q3_mom_por_canal(model),
        "q4_margen_negativo": q4_margen_negativo(model),
        "resumen":            resumen_ejecutivo(model),
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import sys
    sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))
    from ingest import load_all
    from transform import transform_all

    raw   = load_all()
    model = transform_all(raw)
    res   = run_all(model)

    print("\n── Q1: Top 10 SKUs por Rotación ──")
    print(res["q1_rotacion"].to_string(index=False))

    print("\n── Q2: Quiebres de Stock >= 3 días (Q1 2026) ──")
    print(res["q2_quiebres"].head(10).to_string(index=False))
    print(f"   ... total: {len(res['q2_quiebres'])} quiebres")

    print("\n── Q3: MoM por Canal ──")
    print(res["q3_mom"].to_string(index=False))

    print("\n── Q4: Margen Negativo ──")
    q4 = res["q4_margen_negativo"]
    print("\n  [ Sistémico ]")
    print(q4["resumen_sistemico"].to_string(index=False))
    print("\n  [ Mixto — tiendas afectadas ]")
    for _, row in q4["resumen_mixto"].iterrows():
        print(f"\n  {row['sku_pos']} — {row['nombre']} "
              f"({row['tiendas_con_margen_negativo']} tienda(s) negativa(s)):")
        for t in row["tiendas_negativas"]:
            print(f"    {t['tienda_id']}  {t['ciudad']:<15}  "
                  f"margen={t['margen_total_mxn']:>10,.0f} MXN  "
                  f"causa={t['posible_causa']}")

    print("\n── Resumen Ejecutivo ──")
    for k, v in res["resumen"].items():
        print(f"  {k}: {v:,}")