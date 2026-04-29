# CaféNorte — Pipeline de Datos

Pipeline de ingesta, normalización y análisis de ventas e inventario para CaféNorte.
Integra 3 fuentes heterogéneas (POS, ERP legacy, Shopify) en un modelo analítico unificado.

## Stack

| Herramienta | Razón |
|---|---|
| **Python 3.12** | Ecosistema maduro, disponible en todas las opciones AWS (Lambda, Glue, EC2) |
| **DuckDB** | Motor SQL embebido, cero infraestructura, maneja 300k+ filas en memoria con facilidad, genera un `.duckdb` portable como entregable |
| **pandas** | Transformaciones de forma y merge_asof para el LOCF de costos |
| **pyarrow** | Lectura de Parquet (Shopify) |

Stack **deliberadamente liviano**: el volumen (~330k filas totales) no justifica Spark ni dbt. Proporcional al problema.

## Estructura

```
cafenorte/
├── data/                    # Fuentes raw (no se commitean en producción)
│   ├── sales.csv
│   ├── inventory.json
│   ├── ecommerce_orders.parquet
│   └── exchange_rates.csv
├── pipeline/
│   ├── ingest.py            # Carga y validación de fuentes
│   ├── transform.py         # Normalización, conciliación, modelo analítico
│   ├── analytics.py         # 4 preguntas de negocio en DuckDB SQL
│   └── persist.py           # Serialización a .duckdb
├── tests/
│   └── test_pipeline.py     # 44 tests (ingest, transform, analytics, integridad)
├── output/
│   └── cafenorte.duckdb     # Modelo analítico persistido (generado al correr)
├── AI_LOG.md                # Bitácora de uso de IA
├── run_pipeline.py          # Punto de entrada
└── requirements.txt
```

## Cómo correr

```bash
# Instalar dependencias
pip install -r requirements.txt

# Correr pipeline completo (genera output/cafenorte.duckdb)
python run_pipeline.py

# Sin persistir a disco
python run_pipeline.py --no-persist

# Tests
python -m pytest tests/test_pipeline.py -v
```

## Supuestos y decisiones de diseño

### Ventas físicas (`sales.csv`)
- Se incluyen `tipo_comprobante == 'I'` (ingreso) y `'E'` (egreso). Los ingresos suman al revenue; los egresos restan como monto negativo para reflejar el ingreso neto real (ventas menos devoluciones/notas de crédito).
- `P` (pagos en parcialidades), `N` (nómina) y `T` (traslados) se excluyen por no representar transacciones de venta con el cliente.
- **Supuesto:** `'E'` sigue la nomenclatura CFDI del SAT. Pendiente confirmar con el cliente.
- → De 86,490 filas raw se procesan **85,597 filas** (82,518 ingresos + 3,079 egresos).

### Product ID canónico (`dim_producto`)
- Se creó un `product_id` unificado que reconcilia los 3 sistemas de identificación: `sku_pos` (POS), `sku_erp` (ERP) y `handle` (Shopify).
- SKUs sin mapeo reciben identificadores sintéticos en lugar de quedar como NULL para no perder registros en la migración:
  - **`CN-UNMAPPED-XXX`** → existen en el ERP pero sin `sku_pos` (10 SKUs)
  - **`EC-UNMAPPED-XXX`** → existen en Shopify sin mapeo al ERP (6 handles)
- `fact_ventas` y `fact_inventario` conservan `sku_pos` y `sku_erp` originales para trazabilidad.

### Costo vigente (margen)
- Para cada venta se aplica el costo más reciente cuya `fecha_vigencia <= fecha_venta` (**LOCF — Last Observation Carried Forward**).
- Si no hay costo previo a la venta, se usa el costo más antiguo disponible (supuesto conservador).

### Conversión de moneda (e-commerce)
- Todos los montos se convierten a MXN usando el tipo de cambio del día exacto.
- `exchange_rates.csv` cubre Abr 2025 – Mar 2026; el e-commerce también inicia en Abr 2025 → sin gap.

### Quiebres de stock
- `cantidad_en_stock == 0` por un día = un día de quiebre.
- Los snapshots de inventario cubren **Oct 2025 – Mar 2026**.
- "Último trimestre" se interpreta como **Q1 2026 (Ene–Mar 2026)**.
- Se reportan rachas de **>= 3 días consecutivos** (inclusive).

### Rotación de inventario
- Se incluyen **ambos canales** (físico + e-commerce) porque el cliente solicitó visibilidad de todas las tiendas y se asume inventario compartido.
- Tres métricas complementarias: `rotacion_fisica`, `rotacion_extendida` y `pct_ventas_con_inventario`.
- Periodo: **Oct 2025 – Mar 2026** (único solapamiento de inventario + ventas).

---

## Respuestas a las 4 preguntas de negocio

### Q1 — Top 10 SKUs por Rotación de Inventario (Oct 2025 – Mar 2026)

| sku_pos | nombre | categoria | unidades_fisico | unidades_ecommerce | stock_promedio | rotacion_fisica | rotacion_extendida | pct_ventas_con_inventario |
|---|---|---|---|---|---|---|---|---|
| CN-00054 | Espresso Cafe Molido | cafe_molido | 678 | 242 | 39.61 | 17.12 | **23.23** | 73.7% |
| CN-00047 | Filtros Mercancia | mercancia | 686 | 198 | 38.88 | 17.64 | **22.74** | 77.6% |
| CN-00057 | Premium Cafe Grano | cafe_grano | 647 | 225 | 39.16 | 16.52 | **22.27** | 74.2% |
| CN-00038 | Selección Cafe Grano | cafe_grano | 683 | 186 | 39.06 | 17.49 | **22.25** | 78.6% |
| CN-00024 | Americano Cafe Molido | cafe_molido | 603 | 242 | 38.08 | 15.84 | **22.19** | 71.4% |
| CN-00037 | Tradicional Cafe Molido | cafe_molido | 658 | 211 | 39.19 | 16.79 | **22.17** | 75.7% |
| CN-00034 | Estándar Cafe Molido | cafe_molido | 653 | 207 | 39.51 | 16.53 | **21.77** | 75.9% |
| CN-00059 | Orgánico Cafe Grano | cafe_grano | 671 | 189 | 39.65 | 16.92 | **21.69** | 78.0% |
| CN-00010 | Descafeinado Cafe Grano | cafe_grano | 609 | 227 | 38.61 | 15.77 | **21.65** | 72.8% |
| CN-00003 | Filtros Mercancia | mercancia | 635 | 200 | 38.93 | 16.31 | **21.45** | 76.0% |

> `pct_ventas_con_inventario` entre 71-79% indica que todos los productos del top 10 tienen presencia significativa en ambos canales. Ninguno es puramente online ni puramente físico.

---

### Q2 — Quiebres de Stock >= 3 días (Q1 2026: Ene–Mar 2026)

41 quiebres detectados. Los de mayor impacto estimado en ventas perdidas:

| tienda_id | ciudad | region | sku_pos | inicio | fin | días | ventas_perdidas_est. |
|---|---|---|---|---|---|---|---|
| T038 | Cancún | sureste | CN-00060 | 2026-03-15 | 2026-03-17 | 3 | **1.3 uds** |
| T037 | Mérida | sureste | CN-00051 | 2026-02-08 | 2026-02-10 | 3 | 0.6 uds |
| T034 | Puebla | centro | CN-00035 | 2026-03-13 | 2026-03-15 | 3 | 0.6 uds |
| T015 | Reynosa | frontera | CN-00014 | 2026-02-09 | 2026-02-12 | **4** | 0.0 uds |
| T016 | CDMX | centro | CN-00020 | 2026-02-07 | 2026-02-10 | **4** | 0.0 uds |
| T038 | Cancún | sureste | CN-00040 | 2026-03-18 | 2026-03-21 | **4** | 0.0 uds |

> `ventas_perdidas_estimadas` = promedio de unidades diarias en los 14 días previos × días en quiebre. Los quiebres con 0.0 corresponden a SKUs `CN-UNMAPPED` sin historial de ventas en POS.

---

### Q3 — Crecimiento MoM por Canal (Abr 2025 – Mar 2026)

| mes | canal | días | venta_mxn | venta_diaria_prom | variacion_pct_mom | share_ecommerce |
|---|---|---|---|---|---|---|
| 2025-04 | ecommerce | 30 | $383,213 | $12,774 | — | 20.01% |
| 2025-04 | fisico | 30 | $1,532,102 | $51,070 | — | 20.01% |
| 2025-05 | ecommerce | 31 | $365,088 | $11,777 | -7.80% | 18.21% |
| 2025-05 | fisico | 31 | $1,639,658 | $52,892 | +3.57% | 18.21% |
| 2025-06 | ecommerce | 30 | $339,352 | $11,312 | -3.95% | 17.70% |
| 2025-06 | fisico | 30 | $1,577,691 | $52,590 | -0.57% | 17.70% |
| 2025-07 | ecommerce | 31 | $350,578 | $11,309 | -0.02% | 17.57% |
| 2025-07 | fisico | 31 | $1,644,239 | $53,040 | +0.86% | 17.57% |
| 2025-08 | ecommerce | 31 | $343,800 | $11,090 | -1.93% | 17.31% |
| 2025-08 | fisico | 31 | $1,642,587 | $52,987 | -0.10% | 17.31% |
| 2025-09 | ecommerce | 30 | $345,454 | $11,515 | +3.83% | 18.10% |
| 2025-09 | fisico | 30 | $1,563,129 | $52,104 | -1.67% | 18.10% |
| 2025-10 | ecommerce | 31 | $376,497 | $12,145 | +5.47% | 18.60% |
| 2025-10 | fisico | 31 | $1,648,071 | $53,164 | +2.03% | 18.60% |
| 2025-11 | ecommerce | 30 | $359,338 | $11,978 | -1.38% | 18.87% |
| 2025-11 | fisico | 30 | $1,544,897 | $51,497 | -3.14% | 18.87% |
| 2025-12 | ecommerce | 31 | $346,649 | $11,182 | -6.64% | 17.38% |
| 2025-12 | fisico | 31 | $1,648,255 | $53,170 | +3.25% | 17.38% |
| 2026-01 | ecommerce | 31 | $344,899 | $11,126 | -0.50% | 18.17% |
| 2026-01 | fisico | 31 | $1,553,377 | $50,109 | -5.76% | 18.17% |
| 2026-02 | ecommerce | 28 | $323,251 | $11,545 | +3.76% | 18.39% |
| 2026-02 | fisico | 28 | $1,434,265 | $51,224 | +2.22% | 18.39% |
| 2026-03 | ecommerce | 31 | $341,067 | $11,002 | -4.70% | 18.51% |
| 2026-03 | fisico | 31 | $1,501,191 | $48,426 | -5.46% | 18.51% |

> La variación MoM se calcula sobre `venta_diaria_promedio` para evitar el sesgo de meses con distinto número de días. El share de e-commerce se mantiene estable entre 17-20% durante todo el año.

---

### Q4 — Productos con Margen Negativo

**Sistémico** — margen negativo en toda la cadena:

| sku_pos | nombre | tiendas_negativas | margen_total_mxn | precio_venta_prom | costo_unitario_prom | brecha |
|---|---|---|---|---|---|---|
| CN-00015 | Especial Cafe Molido | 40/40 | **-$282,903** | $332.21 | $474.97 | $142.76 |
| CN-00002 | Sándwich Comida Caliente | 40/40 | **-$84,589** | $123.19 | $169.05 | $45.87 |

**Mixto** — negativo en algunas tiendas, positivo en total (11 SKUs, muestra):

| sku_pos | nombre | tiendas_negativas | posible_causa principal |
|---|---|---|---|
| CN-00007 | Selección Cafe Grano | 3/41 | merma_operativa (T012 Mexicali -$1,847, T021 León -$422) |
| CN-00044 | Filtros Mercancia | 2/40 | precio_regional_bajo (T005 Querétaro -$1,207, T033 Monterrey -$409) |
| CN-00019 | Gourmet Cafe Molido | 1/41 | merma_operativa (T025 Chihuahua -$1,536) |
| CN-00059 | Orgánico Cafe Grano | 1/41 | precio_regional_bajo (T038 Cancún -$920) |
| CN-00010 | Descafeinado Cafe Grano | 1/41 | precio_regional_bajo (T031 CDMX -$631) |

> **Causas inferidas:** `precio_regional_bajo` = precio local >10% menor al promedio nacional del SKU. `merma_operativa` = muchas transacciones sostenidas con margen negativo. `promocion_mal_configurada` = pocas transacciones y unidades (caso puntual).

---

## Resumen ejecutivo

| KPI | Valor |
|---|---|
| Revenue total (MXN) | $32,590,967 |
| Canal físico | $28,362,085 (87.0%) |
| Canal e-commerce | $4,228,883 (13.0%) |
| Transacciones totales | 95,544 |
| Tiendas activas | 40 |
| SKUs activos | 76 |
| Tests | 44 / 44 ✓ |
| Tiempo de ejecución | ~10 segundos |
| Base de datos generada | cafenorte.duckdb (24.51 MB) |



# En caso de querer vizualisar los datos en dashboard, correr el app.py

pip install streamlit altair

python -m streamlit run app.py