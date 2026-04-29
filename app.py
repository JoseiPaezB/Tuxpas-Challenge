import streamlit as st
import pandas as pd
import altair as alt

from pipeline.ingest import load_all
from pipeline.transform import transform_all
from pipeline.analytics import (
    q1_top10_rotacion,
    q2_quiebres_stock,
    q3_mom_por_canal,
    q4_margen_negativo,
)

st.set_page_config(
    page_title="CaféNorte Analytics",
    page_icon="☕",
    layout="wide",
)

@st.cache_data
def load_model():
    raw = load_all()
    return transform_all(raw)


def money(value):
    return f"${value:,.0f} MXN"


model = load_model()
ventas = model["fact_ventas"]

st.title("☕ CaféNorte Analytics Dashboard")
st.caption("Vista ejecutiva para ventas, inventario, rotación, stockouts y margen")

# ─────────────────────────────
# KPIs generales
# ─────────────────────────────

col1, col2, col3 = st.columns(3)

col1.metric("Ventas totales", money(ventas["monto_mxn"].sum()))
col2.metric("Transacciones", f"{len(ventas):,}")
col3.metric("Canales", ventas["canal"].nunique())

st.divider()

tab1, tab2, tab3, tab4 = st.tabs([
    "Rotación",
    "Stockouts",
    "MoM por Canal",
    "Margen Negativo",
])

# ─────────────────────────────
# Q1 — Rotación
# ─────────────────────────────

with tab1:
    st.header("Top SKUs por rotación")
    st.write(
        "Productos que rotan más rápido comparando unidades vendidas "
        "contra inventario promedio."
    )

    q1 = q1_top10_rotacion(model)
    top3_q1 = q1.head(3)

    st.subheader("🔝 Top 3 productos clave")
    st.dataframe(top3_q1, use_container_width=True, hide_index=True)

    chart_q1 = (
        alt.Chart(top3_q1)
        .mark_bar(size=35)
        .encode(
            x=alt.X("rotacion_extendida:Q", title="Rotación extendida"),
            y=alt.Y("sku_pos:N", sort="-x", title="SKU"),
            color=alt.Color(
                "rotacion_extendida:Q",
                scale=alt.Scale(scheme="blues"),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("sku_pos:N", title="SKU"),
                alt.Tooltip("rotacion_extendida:Q", title="Rotación extendida", format=".2f"),
                alt.Tooltip("rotacion_fisica:Q", title="Rotación física", format=".2f"),
                alt.Tooltip("pct_ventas_con_inventario:Q", title="% ventas con inventario", format=".1f"),
            ],
        )
        .properties(height=220)
    )

    st.altair_chart(chart_q1, use_container_width=True)

    with st.expander("Ver Top 10 completo"):
        st.dataframe(q1, use_container_width=True, hide_index=True)

# ─────────────────────────────
# Q2 — Stockouts
# ─────────────────────────────

with tab2:
    st.header("Quiebres de stock mayores a 3 días")
    st.write(
        "Tiendas y productos que estuvieron sin inventario durante varios días "
        "consecutivos en el último trimestre."
    )

    q2 = q2_quiebres_stock(model)

    if q2.empty:
        st.success("No se detectaron quiebres relevantes.")
    else:
        top3_q2 = q2.head(3)

        st.subheader("🚨 Top 3 quiebres más críticos")
        st.dataframe(top3_q2, use_container_width=True, hide_index=True)

        chart_q2 = (
            alt.Chart(top3_q2)
            .mark_bar(size=35)
            .encode(
                x=alt.X("ventas_perdidas_estimadas:Q", title="Ventas perdidas estimadas"),
                y=alt.Y("sku_pos:N", sort="-x", title="SKU"),
                color=alt.value("#c0392b"),
                tooltip=[
                    alt.Tooltip("tienda_id:N", title="Tienda"),
                    alt.Tooltip("sku_pos:N", title="SKU"),
                    alt.Tooltip("inicio_quiebre:T", title="Inicio"),
                    alt.Tooltip("fin_quiebre:T", title="Fin"),
                    alt.Tooltip("dias_en_quiebre:Q", title="Días"),
                    alt.Tooltip(
                        "ventas_perdidas_estimadas:Q",
                        title="Ventas perdidas estimadas",
                        format=",.2f",
                    ),
                ],
            )
            .properties(height=220)
        )

        st.altair_chart(chart_q2, use_container_width=True)

        with st.expander("Ver todos los quiebres"):
            st.dataframe(q2, use_container_width=True, hide_index=True)

# ─────────────────────────────
# Q3 — MoM por canal
# ─────────────────────────────

with tab3:
    st.header("Crecimiento mes a mes por canal")
    st.write(
        "Comparación de ventas mensuales entre canal físico y e-commerce "
        "durante el último año disponible."
    )

    q3 = q3_mom_por_canal(model).copy()
    q3["mes"] = pd.to_datetime(q3["mes"])

    st.subheader("📈 Tendencia mensual")

    chart_q3 = (
        alt.Chart(q3)
        .mark_line(point=True)
        .encode(
            x=alt.X("mes:T", title="Mes"),
            y=alt.Y("venta_mxn:Q", title="Ventas MXN"),
            color=alt.Color("canal:N", title="Canal"),
            tooltip=[
                alt.Tooltip("mes:T", title="Mes", format="%Y-%m"),
                alt.Tooltip("canal:N", title="Canal"),
                alt.Tooltip("venta_mxn:Q", title="Ventas MXN", format=",.0f"),
                alt.Tooltip("variacion_pct:Q", title="Variación MoM %", format=".2f"),
            ],
        )
        .properties(height=320)
    )

    st.altair_chart(chart_q3, use_container_width=True)

    st.subheader("🔍 Últimos 3 meses por canal")
    ultimos_q3 = q3.sort_values("mes").groupby("canal").tail(3)

    st.dataframe(ultimos_q3, use_container_width=True, hide_index=True)

    if "share_ecommerce_pct" in q3.columns:
        st.subheader("📊 Participación e-commerce")

        share = q3[["mes", "share_ecommerce_pct"]].drop_duplicates()

        chart_share = (
            alt.Chart(share)
            .mark_line(point=True)
            .encode(
                x=alt.X("mes:T", title="Mes"),
                y=alt.Y("share_ecommerce_pct:Q", title="Share e-commerce %"),
                color=alt.value("#2c7fb8"),
                tooltip=[
                    alt.Tooltip("mes:T", title="Mes", format="%Y-%m"),
                    alt.Tooltip("share_ecommerce_pct:Q", title="Share ecommerce %", format=".2f"),
                ],
            )
            .properties(height=260)
        )

        st.altair_chart(chart_share, use_container_width=True)

# ─────────────────────────────
# Q4 — Margen negativo
# ─────────────────────────────

with tab4:
    st.header("Productos con margen negativo")
    st.write(
        "Productos cuyo margen acumulado es negativo. "
        "El foco principal está en problemas sistémicos a nivel cadena."
    )

    q4 = q4_margen_negativo(model)

    detalle = q4["detalle_por_tienda"].copy()
    resumen_sistemico = q4["resumen_sistemico"].copy()
    resumen_mixto = q4["resumen_mixto"].copy()

    # ─────────────────────────────
    # Foco principal: problemas sistémicos
    # ─────────────────────────────

    st.subheader("📌 Problemas sistémicos por SKU")
    st.write(
        "SKUs que pierden dinero a nivel cadena completa. "
        "Estos son los casos prioritarios para revisar pricing, costos o proveedores."
    )

    if resumen_sistemico.empty:
        st.success("No se encontraron SKUs con margen negativo sistémico.")
    else:
        top_sistemico = (
            resumen_sistemico
            .sort_values("margen_total_mxn")
            .head(5)
        )

        st.dataframe(
            top_sistemico,
            use_container_width=True,
            hide_index=True,
        )

        chart_sistemico = (
            alt.Chart(top_sistemico)
            .mark_bar(size=35)
            .encode(
                x=alt.X("margen_total_mxn:Q", title="Margen total MXN"),
                y=alt.Y("sku_pos:N", sort="x", title="SKU"),
                color=alt.value("#c0392b"),
                tooltip=[
                    alt.Tooltip("sku_pos:N", title="SKU"),
                    alt.Tooltip("nombre:N", title="Producto"),
                    alt.Tooltip("categoria:N", title="Categoría"),
                    alt.Tooltip("margen_total_mxn:Q", title="Margen total", format=",.0f"),
                    alt.Tooltip("ingreso_total_mxn:Q", title="Ingreso total", format=",.0f"),
                    alt.Tooltip("costo_total_mxn:Q", title="Costo total", format=",.0f"),
                    alt.Tooltip("margen_pct:Q", title="Margen %", format=".2f"),
                ],
            )
            .properties(height=280)
        )

        st.altair_chart(chart_sistemico, use_container_width=True)

    # ─────────────────────────────
    # Detalle secundario
    # ─────────────────────────────

    with st.expander("Ver detalle por tienda"):
        if detalle.empty:
            st.info("No hay detalle por tienda con margen negativo.")
        else:
            st.dataframe(
                detalle.sort_values("margen_total_mxn"),
                use_container_width=True,
                hide_index=True,
            )

    with st.expander("Ver problemas mixtos"):
        st.write(
            "SKUs con margen total positivo, pero con pérdidas en algunas tiendas."
        )

        if resumen_mixto.empty:
            st.info("No se encontraron problemas mixtos.")
        else:
            st.dataframe(
                resumen_mixto,
                use_container_width=True,
                hide_index=True,
            )