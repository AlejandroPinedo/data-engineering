# Estructura y Contenido de las Presentaciones: Proyecto Final
## Maestría en Ciencia de Datos e Inteligencia Artificial - UTEC

Este documento detalla la estructura y el contenido exacto diapositiva por diapositiva para los entregables de presentación del proyecto final, incorporando la automatización mediante **Databricks Jobs (Workflows)** y la documentación detallada de las columnas del **Diccionario de Datos** de la capa Gold para el grupo **g102** (G2 de la Sección 1).

---

## Parte A: Presentación Ejecutiva (Máximo 10 Diapositivas)
*Enfoque: Directo, conceptual, visual, sin código. Formato Why - How - What - When.*

### 🛝 Diapositiva 1: Carátula
* **Título:** Pipeline de Ingesta y Exposición de Data Products para Control de Inventario en Consignación
* **Subtítulo:** Proyecto Integrador de Ingeniería de Datos (Etapa Final)
* **Empresa:** Compañía Minera Las Bambas S.A.
* **Integrantes (Grupo 2 - Sección 1):**
  * David Jimenez
  * Herles Pinedo
  * Jose Nomberto
  * Kevin Luyo
  * Yessenia Chirinos

### 🛝 Diapositiva 2: El Contexto (El Negocio de Las Bambas)
* **Título:** Logística de Repuestos Críticos y Capital de Trabajo
* **Mensaje Clave:** En operaciones mineras a gran escala, la disponibilidad de herramientas, consumibles de seguridad (EPP) y repuestos mecánicos es vital para asegurar la continuidad operativa.
* **Consignación:** Los materiales se almacenan en la mina, pero pertenecen legalmente a proveedores clave (Ferreyros, Komatsu, Shell, Epiroc, Moly-Cop) hasta que la mina los consume, momento en el cual se efectúa la compra automática.

### 🛝 Diapositiva 3: WHY (¿Por qué estamos haciendo esto?)
* **Título:** El Desafío Financiero y Operativo
* **Dolor de Negocio:**
  * **Stockouts (Desabastecimiento):** La falta de un repuesto crítico paraliza la planta o frentes de minado, con pérdidas estimadas de **$10,000 a $50,000 USD por hora**.
  * **Falta de visibilidad:** El control de inventarios se realiza en SAP local. Los proveedores reciben reportes manuales por correo de forma semanal/quincenal.
  * **Consecuencias:** Desabastecimiento por reacción tardía o sobrestock preventivo ineficiente de los proveedores en los almacenes físicos de la mina.

### 🛝 Diapositiva 4: HOW (¿Cómo lo estamos solucionando?)
* **Título:** Arquitectura de Datos Desacoplada y Distribuida
* **Arquitectura de Ingesta (Airflow + Azure):** Ingesta automática diaria e inmutable de los reportes SAP en local a Azure Data Lake Storage (capa Raw, ruta: `raw/airflow/G2`).
* **Orquestación en Databricks Workflows:** Automatización end-to-end estructurada en tareas secuenciales programadas diariamente (Bronze -> Silver -> Gold -> Refresco/Auditoría).
* **Arquitectura Medallion en Databricks:**
  * **Bronze:** Consolidación histórica 1:1 de los archivos.
  * **Silver:** Calidad (deduplicación, enmascaramiento PII) y enriquecimiento con el maestro de materiales; desvío automático de errores a **Cuarentena**.
  * **Gold:** Modelado de negocio y consolidación de los **3 Data Products**.

### 🛝 Diapositiva 5: WHAT (¿Qué construimos?)
* **Título:** Descentralización a través de Data Products
* **Data Product 1 - Alertas de Reabastecimiento Crítico (`gold_replenishment_alerts`):** Calcula el stock actual agrupando ingresos y consumos de consignación. Dispara alertas de compra si baja del punto de reorden.
* **Data Product 2 - Análisis de Consumo y Costos (`gold_consumption_analysis`):** Visualiza consumos mensuales históricos para renegociar contratos.
* **Data Product 3 - Conciliación Mensual (`gold_monthly_conciliation`):** Detalla los consumos facturables de fin de mes, automatizando el proceso de conciliación contable.

### 🛝 Diapositiva 6: WHAT (Aplicación de Data Mesh y Gobierno del Dato)
* **Título:** Gobierno del Dato y Descentralización de la Propiedad
* **Propiedad por Dominio:**
  * Catálogos independientes creados en Unity Catalog mediante el registro oficial de la clase: `g102_log_reabastecimiento`, `g102_log_consumos`, `g102_fin_conciliacion`.
* **Gobernanza Computacional Federada:**
  * **Seguridad PII:** Enmascaramiento dinámico a nivel de columna (UDF SQL) sobre el campo `User Name` (Usuario SAP de mina) para restringir el acceso a no administradores.
  * **Contratos de Datos:** Definición automática de esquemas y reglas de calidad en YAML para garantizar la fiabilidad del Data Product.

### 🛝 Diapositiva 7: WHAT (Valor de Negocio Generado)
* **Título:** Impacto en la Cadena de Abastecimiento
* **Reducción de Paradas de Planta:** Alerta proactiva que reduce el riesgo de stockouts críticos en un **90%**.
* **Eficiencia Financiera:** Reducción del sobrestock preventivo en almacén, liberando espacio físico y optimizando el capital de trabajo de los proveedores.
* **Productividad Administrativa:** Ahorro de más de **20 horas semanales** de trabajo manual de conciliación por parte del equipo logístico y financiero.

### 🛝 Diapositiva 8: WHEN (Estado Actual y Hoja de Ruta)
* **Título:** Plan de Despliegue y Resultados del MVP
* **Hitos Completados:**
  * Ingesta automática local a nube (Airflow DAG estable en Azure).
  * Procesamiento Medallion y generación de los 3 Data Products en Databricks mediante **Vistas Materializadas** para refresco incremental optimizado.
  * Publicación de tablas y vistas en Unity Catalog.
  * Monitoreo y control de calidad mediante consultas de uso en `system.query.history`.
* **Hitos por Ejecutar (Q3 2026):**
  * Conexión directa del catálogo de Databricks con Power BI para tableros dinámicos.
  * Capacitación a los planificadores de proveedores en el consumo de las vistas.

### 🛝 Diapositiva 9: Lecciones Aprendidas y Desafíos
* **Título:** Retos Superados en el Camino
* **Calidad de Origen:** SAP presentaba registros con descripciones inconsistentes y valores nulos en columnas de control. Se resolvió creando el maestro de materiales dinámico y la tabla de **Cuarentena**.
* **Gobernanza:** Definir reglas de seguridad para que ningún proveedor pueda ver la rotación ni stock de sus competidores, logrando aislamiento total en Gold.
* **Data Mesh:** La transición cultural de tratar los datos como un reporte a tratarlos como un **Producto de Datos** mantenido por el dominio de Logística.

### 🛝 Diapositiva 10: Cierre
* **Texto:** ¡Muchas gracias! ¿Tienen alguna pregunta?
* **Contacto:** Equipo de Ingeniería de Datos - Las Bambas.

---

## Parte B: Estructura de la Presentación Técnica
*Enfoque: Detalles de implementación, código, diagramas de arquitectura y especificaciones del modelo.*

### 📌 Contenido Técnico Relevante
1. **Diagrama Técnico de Arquitectura:** Detalle del DAG en Airflow, WasbHook, almacenamiento en ADLS Gen2, y flujo de procesamiento Delta Lake en Databricks.
2. **Orquestación con Databricks Jobs (Workflows):**
   * Topología del Job secuencial programado de forma diaria (`America/Lima`).
   * Configuración de Job Cluster, políticas de reintentos y notificaciones en caso de fallos.
3. **Modelo de Datos de Capa Silver:** 
   * Estructura de `silver_movimientos_consignacion`
   * Estructura de `silver_movimientos_cuarentena` (Mapeo de fallas de calidad)
   * Lógica de enmascaramiento dinámico (PII Masking) para el campo `User Name` (Usuario responsable del movimiento).
4. **Modelado Físico y Diccionario de Datos de Data Products (Gold):**
   * Creación de **Vistas Materializadas** (`MATERIALIZED VIEW`) en Databricks Unity Catalog para almacenamiento y actualización física incremental eficiente.
   * Lógica de agregación y cálculo del stock acumulado.
   * **Diccionario de datos detallado columna por columna** (`COMMENT ON COLUMN`) para las 3 vistas materializadas de Gold.
5. **Gobernanza y SLOs en Unity Catalog:**
   * Script de creación de esquemas y políticas de seguridad (Row-Level Security / Column-Level Security).
   * Monitoreo del uso de los Data Products del grupo **g102** consultando la tabla `system.query.history`.
   * Ejemplo de Contrato de Datos YAML del Data Product.
6. **Logs de Ejecución:** Pantallazos de la corrida del pipeline en Databricks Workflows y visualización de las vistas materializadas resultantes en el panel de gobernanza de Unity Catalog.
