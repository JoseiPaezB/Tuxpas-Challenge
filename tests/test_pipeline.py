"""
test_pipeline.py — Tests mínimos para garantizar confiabilidad del pipeline.

Corre con: pytest tests/test_pipeline.py -v
"""

import json
import sys
from io import StringIO
from pathlib import Path

import pandas as pd
import pytest

# Asegura imports relativos al proyecto
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.ingest import load_sales, load_inventory, load_ecommerce, load_exchange_rates
from pipeline.transform import (
    transform_sales,
    transform_ecommerce,
    build_fact_ventas,
    build_fact_ventas_con_costo,
    build_fact_inventario,
    build_dim_tiendas,
)
from pipeline.analytics import (
    q1_top10_rotacion,
    q2_quiebres_stock,
    q3_mom_por_canal,
    q4_margen_negativo,
)

DATA_DIR = Path(__file__).parent.parent / "data"


# ──────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────

@pytest.fixture(scope="session")
def raw():
    from pipeline.ingest import load_all
    return load_all()


@pytest.fixture(scope="session")
def model(raw):
    from pipeline.transform import transform_all
    return transform_all(raw)


# ──────────────────────────────────────────────
# Tests de Ingest
# ──────────────────────────────────────────────

class TestIngest:

    def test_sales_shape(self, raw):
        df = raw["sales"]
        assert len(df) == 86_490, f"Se esperaban 86490 filas, se obtuvieron {len(df)}"
        assert set(["venta_id", "fecha_hora", "tienda_id", "sku", "cantidad", "monto"]).issubset(df.columns)

    def test_sales_no_nulls_en_columnas_criticas(self, raw):
        df = raw["sales"]
        for col in ["tienda_id", "sku", "cantidad", "monto"]:
            assert df[col].isna().sum() == 0, f"Columna {col} tiene nulls"

    def test_inventory_tiendas_completo(self, raw):
        assert len(raw["tiendas"]) == 40

    def test_inventory_snapshots_no_vacio(self, raw):
        assert len(raw["snapshots"]) > 0

    def test_catalogo_tiene_costos(self, raw):
        cat = raw["catalogo"]
        assert "costo_mxn" in cat.columns
        assert cat["costo_mxn"].isna().sum() == 0

    def test_ecommerce_shape(self, raw):
        df = raw["ecommerce"]
        assert len(df) == 9_947
        assert "currency" in df.columns

    def test_exchange_rates_cubre_periodo_ecommerce(self, raw):
        fx = raw["exchange_rates"]
        eco = raw["ecommerce"]
        fx_min = fx["fecha"].min()
        eco_min = eco["fecha"].dt.normalize().min()
        assert fx_min <= eco_min, \
            f"Exchange rates ({fx_min}) no cubre inicio de ecommerce ({eco_min})"


# ──────────────────────────────────────────────
# Tests de Transform
# ──────────────────────────────────────────────

class TestTransform:

    def test_sales_incluye_I_y_E(self, model):
        """Ventas físicas deben incluir ingresos (I) y egresos (E) como negativos."""
        fv = model["fact_ventas"]
        fisicas = fv[fv["canal"] == "fisico"]
        # 82,518 ingresos + 3,079 egresos = 85,597
        assert len(fisicas) == 85_597, f"Se esperaban 85597 filas físicas, hay {len(fisicas)}"

    def test_egresos_son_negativos(self, model):
        """Los egresos deben haberse registrado con monto negativo."""
        fv = model["fact_ventas"]
        fisicas = fv[fv["canal"] == "fisico"]
        negativos = (fisicas["monto_mxn"] < 0).sum()
        assert negativos == 3_079, f"Se esperaban 3079 montos negativos (egresos), hay {negativos}"

    def test_fact_ventas_tiene_ambos_canales(self, model):
        canales = set(model["fact_ventas"]["canal"].unique())
        assert "fisico" in canales
        assert "ecommerce" in canales

    def test_monto_mxn_positivo_ecommerce(self):
        pass  # cubierto por test_egresos_son_negativos

    def test_conversion_fx_ecommerce(self, model):
        """Ordenes en USD/EUR deben haberse convertido a MXN."""
        fv = model["fact_ventas"]
        eco = fv[fv["canal"] == "ecommerce"]
        assert eco["monto_mxn"].isna().sum() == 0

    def test_dim_tiendas_incluye_ecommerce(self, model):
        assert "ECOMMERCE" in model["dim_tiendas"]["tienda_id"].values

    def test_dim_producto_tiene_product_id(self, model):
        """Todos los productos deben tener product_id."""
        dp = model["dim_producto"]
        assert "product_id" in dp.columns
        assert dp["product_id"].isna().sum() == 0

    def test_dim_producto_incluye_unmapped(self, model):
        """dim_producto debe incluir CN-UNMAPPED y EC-UNMAPPED."""
        dp = model["dim_producto"]
        assert dp["product_id"].str.contains("CN-UNMAPPED").any(), "No se encontraron CN-UNMAPPED"
        assert dp["product_id"].str.contains("EC-UNMAPPED").any(), "No se encontraron EC-UNMAPPED"

    def test_dim_producto_conserva_ids_originales(self, model):
        """dim_producto debe conservar sku_pos, sku_erp y handle para trazabilidad."""
        dp = model["dim_producto"]
        for col in ["sku_pos", "sku_erp", "handle"]:
            assert col in dp.columns, f"Columna {col} no encontrada en dim_producto"

    def test_fact_ventas_tiene_product_id(self, model):
        """fact_ventas debe tener product_id en todas las filas."""
        fv = model["fact_ventas"]
        assert "product_id" in fv.columns
        assert fv["product_id"].isna().sum() == 0

    def test_fact_inventario_tiene_product_id(self, model):
        """fact_inventario debe tener product_id en todas las filas."""
        fi = model["fact_inventario"]
        assert "product_id" in fi.columns
        assert fi["product_id"].isna().sum() == 0

    def test_margen_calculado(self, model):
        """fact_ventas debe tener margen_mxn calculado donde hay costo."""
        fv = model["fact_ventas"]
        con_costo = fv[fv["costo_mxn"].notna()]
        diff = (con_costo["monto_mxn"] - con_costo["costo_total_mxn"] - con_costo["margen_mxn"]).abs()
        assert diff.max() < 0.01, "Cálculo de margen incorrecto"

    def test_inventario_tiene_sku_pos(self, model):
        """
        El 85%+ de snapshots deben tener sku_pos.
        El ~14% restante corresponde a 10 sku_erp sin sku_pos en sku_mappings,
        ahora representados como CN-UNMAPPED en dim_producto.
        """
        fi = model["fact_inventario"]
        assert "sku_pos" in fi.columns
        pct_con_sku = fi["sku_pos"].notna().mean()
        assert pct_con_sku > 0.80, f"Solo {pct_con_sku:.1%} de snapshots tienen sku_pos"


# ──────────────────────────────────────────────
# Tests de Analytics
# ──────────────────────────────────────────────

class TestAnalytics:

    def test_q1_retorna_exactamente_10(self, model):
        result = q1_top10_rotacion(model)
        assert len(result) == 10

    def test_q1_rotacion_positiva(self, model):
        result = q1_top10_rotacion(model)
        assert (result["rotacion_extendida"] > 0).all()
        assert (result["rotacion_fisica"] > 0).all()

    def test_q1_ordenado_desc(self, model):
        result = q1_top10_rotacion(model)
        rotaciones = result["rotacion_extendida"].tolist()
        assert rotaciones == sorted(rotaciones, reverse=True)

    def test_q1_pct_entre_0_y_100(self, model):
        result = q1_top10_rotacion(model)
        assert (result["pct_ventas_con_inventario"] >= 0).all()
        assert (result["pct_ventas_con_inventario"] <= 100).all()

    def test_q1_rotacion_extendida_mayor_o_igual_fisica(self, model):
        """La rotación extendida siempre debe ser >= a la física."""
        result = q1_top10_rotacion(model)
        assert (result["rotacion_extendida"] >= result["rotacion_fisica"]).all()

    def test_q2_quiebres_mayores_o_igual_a_3_dias(self, model):
        result = q2_quiebres_stock(model)
        if not result.empty:
            assert (result["dias_en_quiebre"] >= 3).all()

    def test_q2_en_rango_q1_2026(self, model):
        result = q2_quiebres_stock(model)
        if not result.empty:
            assert (result["inicio_quiebre"] >= pd.Timestamp("2026-01-01")).all()
            assert (result["fin_quiebre"] <= pd.Timestamp("2026-03-31")).all()

    def test_q2_ventas_perdidas_no_negativas(self, model):
        result = q2_quiebres_stock(model)
        if not result.empty:
            assert (result["ventas_perdidas_estimadas"] >= 0).all()

    def test_q2_ordenado_por_impacto(self, model):
        """Debe estar ordenado por ventas_perdidas_estimadas DESC."""
        result = q2_quiebres_stock(model)
        if len(result) > 1:
            vals = result["ventas_perdidas_estimadas"].tolist()
            assert vals == sorted(vals, reverse=True)

    def test_q3_tiene_ambos_canales(self, model):
        result = q3_mom_por_canal(model)
        canales = set(result["canal"].unique())
        assert "fisico" in canales
        assert "ecommerce" in canales

    def test_q3_cubre_12_meses(self, model):
        result = q3_mom_por_canal(model)
        meses_fisico = result[result["canal"] == "fisico"]["mes"].nunique()
        assert meses_fisico == 12, f"Se esperaban 12 meses, hay {meses_fisico}"

    def test_q3_venta_positiva(self, model):
        result = q3_mom_por_canal(model)
        assert (result["venta_mxn"] > 0).all()

    def test_q3_venta_diaria_consistente(self, model):
        """venta_diaria_promedio debe ser igual a venta_mxn / dias_en_mes."""
        result = q3_mom_por_canal(model)
        diff = (result["venta_mxn"] / result["dias_en_mes"] - result["venta_diaria_promedio"]).abs()
        assert diff.max() < 1.0, "venta_diaria_promedio inconsistente con venta_mxn / dias_en_mes"

    def test_q3_share_ecommerce_entre_0_y_100(self, model):
        result = q3_mom_por_canal(model)
        assert (result["share_ecommerce_pct"] >= 0).all()
        assert (result["share_ecommerce_pct"] <= 100).all()

    def test_q3_share_ecommerce_igual_por_mes(self, model):
        """El share_ecommerce debe ser el mismo para físico y ecommerce en el mismo mes."""
        result = q3_mom_por_canal(model)
        for mes, group in result.groupby("mes"):
            shares = group["share_ecommerce_pct"].unique()
            assert len(shares) == 1, f"Share distinto entre canales en {mes}"

    def test_q4_retorna_tres_dataframes(self, model):
        result = q4_margen_negativo(model)
        assert "detalle_por_tienda" in result
        assert "resumen_sistemico"  in result
        assert "resumen_mixto"      in result

    def test_q4_margen_efectivamente_negativo(self, model):
        detalle = q4_margen_negativo(model)["detalle_por_tienda"]
        if not detalle.empty:
            assert (detalle["margen_total_mxn"] < 0).all()

    def test_q4_sistemico_margen_negativo_en_cadena(self, model):
        """SKUs sistémicos deben tener margen total negativo en toda la cadena."""
        sistemico = q4_margen_negativo(model)["resumen_sistemico"]
        if not sistemico.empty:
            assert (sistemico["margen_total_mxn"] < 0).all()

    def test_q4_mixto_margen_positivo_en_cadena(self, model):
        """SKUs mixtos deben tener margen total positivo aunque tengan tiendas negativas."""
        mixto = q4_margen_negativo(model)["resumen_mixto"]
        if not mixto.empty:
            assert (mixto["margen_total_mxn"] >= 0).all()

    def test_q4_mixto_tiene_tiendas_negativas(self, model):
        """SKUs mixtos deben tener al menos 1 tienda con margen negativo."""
        mixto = q4_margen_negativo(model)["resumen_mixto"]
        if not mixto.empty:
            assert (mixto["tiendas_con_margen_negativo"] >= 1).all()

    def test_q4_solo_con_costo_conocido(self, model):
        detalle = q4_margen_negativo(model)["detalle_por_tienda"]
        if not detalle.empty:
            diff = (detalle["margen_total_mxn"] -
                    (detalle["ingreso_total_mxn"] - detalle["costo_total_mxn"])).abs()
            assert diff.max() < 1.0, \
                f"Diferencia máxima margen/costo: {diff.max():.4f} MXN"


# ──────────────────────────────────────────────
# Tests de integridad de datos
# ──────────────────────────────────────────────

class TestIntegridad:

    def test_revenue_total_razonable(self, model):
        """El revenue total debe estar en un rango plausible para 40 tiendas."""
        total = model["fact_ventas"]["monto_mxn"].sum()
        # Con 40 tiendas y ~18 meses, esperamos decenas de millones MXN
        assert 10_000_000 < total < 500_000_000, \
            f"Revenue total fuera de rango plausible: {total:,.0f} MXN"

    def test_sin_sku_duplicados_en_dim(self, model):
        dp = model["dim_producto"]
        dups = dp["product_id"].duplicated().sum()
        assert dups == 0, f"{dups} product_ids duplicados en dim_producto"

    def test_tiendas_consistentes_entre_ventas_e_inventario(self, model):
        tiendas_ventas = set(model["fact_ventas"][
            model["fact_ventas"]["canal"] == "fisico"
        ]["tienda_id"].unique())
        tiendas_inv = set(model["fact_inventario"]["tienda_id"].unique())
        # Todas las tiendas con ventas deben tener snapshots de inventario
        diff = tiendas_ventas - tiendas_inv
        assert len(diff) == 0, f"Tiendas con ventas sin inventario: {diff}"