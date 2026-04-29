# AI_LOG.md — Bitácora de Uso de IA
## Proyecto: Pipeline de Datos CaféNorte

---

## 1. Herramientas usadas

| Herramienta | Modelo | Uso principal |
|---|---|---|
| **Claude** (claude.ai) | Claude Sonnet 4.5 | Modelo principal — exploración de datos, construcción del pipeline, tests y documentación |
| **Software Architect GPT** | ChatGPT (branch Software Architect) | Validación de decisiones de arquitectura y diseño propuestas por Claude |
| **Claude** (rama separada) | Claude Sonnet 4.5 | Investigación de servicios AWS y redacción de la propuesta técnica |

---

## 2. Flujo de trabajo

El flujo de trabajo fue bastante dinámico. Empecé por presentar el caso más los archivos. Una vez comprendida la arquitectura del sistema tanto por mí como por la IA, era donde íbamos punto por punto resolviendo. Generalmente, una vez la IA arrojaba un código, lo iba corrigiendo o modificando para hacerlo más acorde o eficiente. También consultaba con diferentes modelos sus opiniones sobre lo que se estaba haciendo. Un ejemplo de esto fue la arquitectura de AWS. La mayor parte del tiempo esto resultaba bueno ya que los modelos daban opiniones similares.

**Claude (rama principal)** se usó para todo el pipeline:
1. Exploración de datos (entender las 4 fuentes antes de escribir código)
2. Diseño del schema (decidir qué tablas construir y cómo nombrarlas)
3. Construcción módulo por módulo: ingest → transform → analytics → persist
4. Tests después de cada módulo
5. Revisión crítica de cada query de negocio antes de cerrarla

Este modelo fue el que más "stress" recibió. Esto se debe a que era el que más conocimiento tenía sobre lo que se estaba haciendo. Se puede decir que este fue el modelo que más se entrenó para resolver la problemática. La conversación fue bastante pausada e interrumpida por preguntas o aclaraciones que le iba haciendo.

**Software Architect GPT** se usó como segunda opinión en decisiones de arquitectura clave — por ejemplo la elección de DuckDB sobre alternativas como dbt o Redshift, y la estructura del modelo dimensional (fact/dim). No generó código, funcionó como validador de las decisiones que Claude proponía. Sin embargo me ayudó a tener más insights sobre la arquitectura de los archivos. Gracias a él me di cuenta de temas como el sistema del SAT para el cobro de ingresos y egresos. También contribuyó en la creación de la interfaz para poder visualizar de mejor manera los resultados. Se podría decir que esta IA sirvió como una segunda opinión y cuestionamiento hacia lo que el modelo principal estaba creando.

**Claude (rama separada)** se usó específicamente para investigar los servicios AWS relevantes para el presupuesto de $200/mes, comparar opciones (Athena vs Redshift Serverless, Glue vs Lambda) y estructurar la propuesta técnica como documento dirigido al cliente. También servía para poder hacer preguntas puntuales sobre librerías o partes del código que se hacían confusas. Por último, utilicé este branch para hacer preguntas más teóricas no solo sobre AWS sino sobre conceptos de negocio y estructuras de despliegue de datos.

No usé agentes autónomos ni MCP servers. Todo fue asistencia directa en conversaciones separadas según el propósito. La razón principalmente se debía a que quería revisar detalladamente lo que cada IA me iba creando, especialmente la rama principal de Claude. Preferí no integrarlo en mi desktop porque de esa manera tenía más libertad sobre el sistema y se volvía más difícil de comprender lo que estaba pasando.

---

## 3. Prompts clave

**Prompt 1 — Exploración inicial de datos**
> "analicemos cada archivo"

La IA corrió exploración sistemática de los 4 archivos: shape, dtypes, nulls, rangos de fechas, valores únicos. Devolvió un diagnóstico completo con los problemas de conciliación ya identificados (3 sistemas de IDs distintos, gap de fechas entre inventario y ventas, 6 handles de Shopify sin mapeo). Lo acepté tal cual — era exactamente lo que necesitaba antes de diseñar el schema.

**Prompt 2 — Diseño del modelo analítico**
> Surgió de la conversación sobre los 3 IDs distintos (sku_pos, sku_erp, handle)

La IA propuso crear un `product_id` canónico que unifique los 3 sistemas. Acepté la dirección pero modifiqué el criterio: en lugar de un ID secuencial arbitrario, preferí basarlo en `sku_pos` para que fuera legible y trazable. También pedí que los SKUs sin mapeo recibieran identificadores `CN-UNMAPPED` y `EC-UNMAPPED` en lugar de quedar como NULL — la IA no había considerado ese caso inicialmente.

**Prompt 3 — Query Q1 de rotación**
> "estas usando las ventas de nada mas el canal fisico?"

Cuestioné la decisión de excluir e-commerce de la rotación. La IA había asumido que como el inventario es del ERP físico, solo debían contar las ventas físicas. Al señalar que el cliente quiere visibilidad de todas las tiendas, la IA ajustó el query para incluir ambos canales y agregó las 3 sub-métricas (rotacion_fisica, rotacion_extendida, pct_ventas_con_inventario). Acepté la propuesta con un ajuste mío: pedir que se mostraran las unidades por canal en el resultado final para más transparencia.

**Prompt 4 — Tipo de comprobante E**
> "no crees que si hay que tomar en cuenta las devoluciones como un monto a favor?"

La IA había excluido los egresos silenciosamente. Al cuestionarlo, reconoció que sí distorsionaba el revenue neto. Acepté el cambio pero antes de aplicarlo le pregunté cómo sabía que `E` significaba devolución — y la IA admitió que lo había asumido basándose en la nomenclatura CFDI del SAT, sin confirmación del cliente. Eso derivó en documentar el supuesto explícitamente en el código y en las preguntas abiertas de la propuesta AWS.

**Prompt 5 — Separación de margen sistémico vs mixto en Q4**
> "la opcion 2 me gusta mas"

La IA había propuesto dos opciones para manejar SKUs con margen negativo en algunas tiendas pero positivo en total. Elegí la opción 2 (mantener ambos pero separarlos). La IA implementó la separación y además agregó la inferencia de `posible_causa` (precio_regional_bajo, promocion_mal_configurada, merma_operativa), y eso sí lo acepté ya que era una lógica sólida y accionable.

---

## 4. Caso donde la IA se equivocó

**El umbral de quiebres de stock.**

La IA escribió `HAVING COUNT(*) > 3` para detectar quiebres de más de 3 días. Yo lo dejé pasar en la primera revisión. Al analizar el resultado (solo 3 quiebres) me pareció que el número era muy bajo y lo cuestioné.

Al revisar con más cuidado, el enunciado dice "más de 3 días" pero en el contexto de negocio lo natural es "3 días o más" — un quiebre de exactamente 3 días es igualmente problemático que uno de 4. La IA había interpretado literalmente el texto en lugar de pensar en el impacto operativo.

También, una vez generado el primer query por parte de la IA, vi que excluyó todos los estatus menos los de N, I y P. Una vez investigué sobre las linealidades del SAT, lo corregí pidiéndole que excluyera esos datos que presentaban una variación hacia lo que realmente era el ingreso de la compañía. También le comenté que agregara el estatus de egreso como una resta al total, de acuerdo a la fórmula ventas totales = ingreso - egreso.

---

## 5. Auto-crítica final

Diría que mi mérito como tal en este proyecto fue de supervisor. Es sencillo nada más copiar y pegar lo que la IA arroja, pero para este tipo de proyectos siempre se debe supervisar claramente lo que se hace. Muchas de las veces nos quedábamos atascados en un simple query o línea de código con la finalidad de mejorar. Diría que la parte que más me tomó tiempo resolver fue el análisis del sistema. Mucha de la conversación fue tratando de comprender qué es lo que quería hacer la IA y si eso era posible, bueno o no.

Lo que es mérito de la IA es la velocidad de implementación y la solidez técnica del código: el island-gap para detectar rachas, el merge_asof para LOCF de costos, la estructura modular del pipeline, y la redacción de la propuesta AWS en lenguaje de cliente no técnico. Yo no habría llegado a esa calidad técnica tan rápido solo.

Para poder evaluar los outputs y el código como tal, le dije a la IA que agregara una sección sofisticada designada solo a hacer tests. En este caso, son más de 44 tests que prueban toda la parcialidad del código. Gracias a esto me prevengo de encontrar errores al momento de ejecutar. También me ayuda a entender mejor cómo está sirviendo la arquitectura del sistema creado.
