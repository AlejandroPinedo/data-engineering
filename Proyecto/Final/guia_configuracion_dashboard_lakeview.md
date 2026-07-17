# Guía Paso a Paso: Configuración del Lakeview Dashboard en Databricks
## Portal de Monitoreo de Inventarios, SLAs y Calidad - Minera Los Andes S.A.

Esta guía detalla el procedimiento exacto para diseñar, enlazar y publicar el **Databricks Lakeview Dashboard** de negocio, estructurado en base a las 4 secciones críticas de KPIs (Alertas de Continuidad, Consumos, Conciliación de Pagos y Gobernanza de Calidad).

---

## Paso 1: Crear el Dashboard y Enlazar los Datasets

1. En la barra lateral izquierda de Databricks, ve a la sección **Dashboards** y haz clic en **Create dashboard** (o ve a tu Workspace, haz clic en **New** ➡️ **Dashboard**).
2. Se abrirá una interfaz con dos pestañas en la parte superior: **Canvas** (Lienzo visual) y **Data** (Fuentes de datos). Selecciona la pestaña **Data**.
3. Haz clic en **Add data** y selecciona **Table or view** para registrar las siguientes 4 fuentes de datos desde tu catálogo `g102_log_reabastecimiento` (o los catálogos correspondientes):

| Nombre del Dataset en Dashboard | Tabla / Vista de Origen | Catálogo y Esquema de Origen |
| :--- | :--- | :--- |
| **Alertas\_Reabastecimiento** | `mv_replenishment_alerts` | `g102_log_reabastecimiento.gold` |
| **Analisis\_Consumos** | `mv_consumption_analysis` | `g102_log_consumos.gold` |
| **Conciliacion\_Pagos** | `mv_monthly_conciliation` | `g102_fin_conciliacion.gold` |
| **Errores\_Cuarentena** | `silver_movimientos_cuarentena` | `g102_log_reabastecimiento.silver` |

---

## Paso 2: Configurar los Filtros Globales (Barra Superior)

En la pestaña **Canvas**:
1. En la barra de herramientas superior del lienzo, selecciona el icono de **Filter** (Filtro) y arrástralo al inicio del lienzo (Fila 1).
2. Haz clic en el filtro creado y en el panel derecho de configuración:
   * **Title:** `Proveedor`
   * **Dataset:** Selecciona **Alertas\_Reabastecimiento**
   * **Field:** `proveedor`
   * En **Fields to filter**, asegúrate de marcar también las columnas `proveedor` de los datasets **Analisis\_Consumos** y **Conciliacion\_Pagos** para que el filtro sea global.
3. Agrega un segundo filtro al costado del anterior:
   * **Title:** `Mes Contable`
   * **Dataset:** Selecciona **Analisis\_Consumos**
   * **Field:** `anio_mes`
   * En **Fields to filter**, mapea el campo `anio_mes` del dataset **Conciliacion\_Pagos**.

---

## Paso 3: Diseñar la Sección 1 - Alertas y Continuidad (DP1)

Agrega un encabezado de texto que diga `### 🚨 1. Alertas de Continuidad Operativa (Evitar Paradas de Planta)`.

### Widget A: Tarjeta KPI - Materiales Críticos
1. Arrastra un componente **Counter (KPI)** al lienzo.
2. En el panel derecho de configuración:
   * **Dataset:** `Alertas_Reabastecimiento`
   * **Value:** `material_id`
   * **Aggregation:** `Count`
   * **Label:** `Materiales en Desabastecimiento Crítico`
   * **Filters / Rules:** Añade una regla para que este contador solo cuente registros donde el campo `estado_inventario` sea igual a `'CRITICO - DESABASTECIMIENTO'`.

### Widget B: Tarjeta KPI - Presupuesto de Reposición Proyectado
1. Arrastra otro componente **Counter (KPI)** al costado del anterior.
2. En el panel derecho de configuración:
   * **Dataset:** `Alertas_Reabastecimiento`
   * **Value:** `costo_reposicion_usd`
   * **Aggregation:** `Sum`
   * **Label:** `Costo de Reposición Proyectado (USD)`

### Widget C: Tabla Detallada de Materiales por Comprar
1. Arrastra un componente **Table** debajo de las dos tarjetas anteriores.
2. En el panel derecho de configuración:
   * **Dataset:** `Alertas_Reabastecimiento`
   * **Columns:** Agrega en orden `material_id`, `descripcion`, `proveedor`, `stock_actual`, `stock_seguridad`, `costo_reposicion_usd`.
   * **Conditional Formatting:** Agrega una regla de formato condicional:
     * *Rule:* Si `stock_actual` $<$ `stock_seguridad`.
     * *Style:* Relleno de la celda o texto en **Rojo claro / Alerta**.

---

## Paso 4: Diseñar la Sección 2 - Análisis de Consumos (DP2)

Agrega un encabezado de texto que diga `### 📊 2. Análisis de Rotación de Inventarios y Costo de Desgaste`.

### Widget A: Gráfico de Barras Apiladas - Tendencia Mensual de Costos
1. Arrastra un componente **Visualization (Chart)**.
2. En el panel derecho de configuración:
   * **Chart type:** `Bar` (Barras columnas)
   * **Dataset:** `Analisis_Consumos`
   * **X-axis:** `anio_mes`
   * **Y-axis:** `costo_consumo_usd` (Aggregation: `Sum`)
   * **Group by (Color):** `categoria`
   * **Stacking:** `Stack` (Apilado)
   * **Title:** `Costo Mensual de Consumo por Categoría de Repuesto`

### Widget B: Gráfico de Anillo - Concentración por Proveedor
1. Arrastra un componente **Visualization (Chart)** al costado del gráfico de barras.
2. En el panel derecho de configuración:
   * **Chart type:** `Pie` (Anillo / Torta)
   * **Dataset:** `Analisis_Consumos`
   * **Angle (Value):** `costo_consumo_usd` (Aggregation: `Sum`)
   * **Grouping (Category):** `proveedor`
   * **Title:** `Participación del Costo de Consumo por Proveedor`

---

## Paso 5: Diseñar la Sección 3 - Conciliación Financiera (DP3)

Agrega un encabezado de texto que diga `### 💵 3. Conciliación Mensual de Facturación Pre-Aprobada`.

### Widget A: Tarjeta KPI - Monto Facturable del Período
1. Arrastra un componente **Counter (KPI)** al lienzo.
2. En el panel derecho de configuración:
   * **Dataset:** `Conciliacion_Pagos`
   * **Value:** `monto_facturable_usd`
   * **Aggregation:** `Sum`
   * **Label:** `Monto Neto Facturable Acumulado (USD)`

### Widget B: Tabla Contable de Detalle Transaccional
1. Arrastra un componente **Table** al costado de la tarjeta anterior o debajo.
2. En el panel derecho de configuración:
   * **Dataset:** `Conciliacion_Pagos`
   * **Columns:** Selecciona `anio_mes`, `proveedor`, `documento_material`, `material_id`, `cantidad_facturable`, `monto_facturable_usd`.
   * **Title:** `Registro de Transacciones Validadas para Facturación`

---

## Paso 6: Diseñar la Sección 4 - Gobernanza y Calidad (Cuarentena)

Agrega un encabezado de texto que diga `### 🛡️ 4. Monitoreo de Gobernanza y Calidad del Dato (Cuarentena)`.

### Widget A: Tarjeta KPI - Registros de SAP Rechazados
1. Arrastra un componente **Counter (KPI)**.
2. En el panel derecho de configuración:
   * **Dataset:** `Errores_Cuarentena`
   * **Value:** `Material_Id`
   * **Aggregation:** `Count`
   * **Label:** `Registros en Cuarentena (Fallo de Ingesta SAP)`

### Widget B: Tabla de Bitácora de Errores
1. Arrastra un componente **Table** debajo del contador.
2. En el panel derecho de configuración:
   * **Dataset:** `Errores_Cuarentena`
   * **Columns:** Selecciona `Material_Id`, `Posting_Date`, `Qty`, `Storage_Location`.
   * **Title:** `Registro Histórico de Registros con Errores Lógicos`

---

## Paso 7: Publicar y Compartir

1. En la esquina superior derecha del Canvas del Dashboard, haz clic en el botón **Publish** (Publicar).
2. En el modal emergente, haz clic en **Publish** para confirmar. Esto creará una versión de consumo inmutable.
3. Haz clic en **Share** (Compartir) en la esquina superior derecha y configura los permisos correspondientes:
   * Agrega a tus integrantes del grupo o profesores.
   * Selecciona el nivel de acceso **Can view** para que puedan interactuar con los filtros y exportar los datos a Excel sin modificar los gráficos.
