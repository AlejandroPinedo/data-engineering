# Guía de Implementación Avanzada: Proyecto Final de Ingeniería de Datos
## Medallion Architecture, Data Mesh & Data Governance en Compañía Minera Las Bambas S.A.

Este documento contiene el plan técnico completo, los códigos **PySpark/SQL** y la guía de configuración del **Databricks Job (Workflow)** para el grupo **G2 de la Sección 1 (Código g102)** utilizando la data real almacenada en el Azure Data Lake.

---

## 1. Fase 0: Registro de los 3 Data Products en la Plataforma

Para que la plataforma cree automáticamente tus 3 catálogos federados en Unity Catalog:

1. Ve a la carpeta compartida de la clase en Databricks y busca la carpeta **`data_product_request`**.
2. Abre el notebook **`submit_request`**.
3. Modifica las variables de la celda de configuración con tus datos reales de grupo y ejecuta el notebook completo:

```python
# Notebook 0: registro_data_products (Configuración dentro del notebook compartido de la clase)

group = "g102"  # Sección 1, Grupo 2 (G2)
leader = "herles.pinedo@utec.edu.pe"  # Correo UTEC del líder del grupo
members = "david.jimenez@utec.edu.pe, jose.nomberto@utec.edu.pe, kevin.luyo@utec.edu.pe, yessenia.chirinos@utec.edu.pe"

# Solicitud de 3 Data Products
data_products = [
    ("log", "logistica", "reabastecimiento"), # Catálogo resultante: g102_log_reabastecimiento
    ("log", "logistica", "consumos"),         # Catálogo resultante: g102_log_consumos
    ("fin", "finanzas",  "conciliacion")       # Catálogo resultante: g102_fin_conciliacion
]

# Ejecutar validación y envío a la base de datos central de solicitudes
submit_dataproduct_request(group, leader, members, data_products)
```

---

## 2. Configuración del Databricks Job (Workflow)

Para automatizar la ejecución diaria del pipeline Medallion, debes crear un **Workflow** en Databricks con la siguiente topología de dependencias:

```
    [Task 1: Bronze_Ingestion] (Notebook 1)
               │
               ▼
    [Task 2: Silver_Processing] (Notebook 2)
               │
               ▼
    [Task 3: Gold_Data_Products] (Notebook 3)
               │
               ▼
    [Task 4: Refresco_Auditoria] (Notebook 4)
```

### Paso a Paso para configurar el Job en Databricks UI:
1. Navega a **Workflows** en la barra lateral izquierda y haz clic en **Create Job**.
2. **Task 1 (Ingesta Bronze):**
   * **Task Name:** `Bronze_Ingestion`
   * **Type:** `Notebook`
   * **Source:** `Workspace`
   * **Path:** Selecciona la ruta de tu `Notebook 1` en tu workspace.
   * **Cluster:** Selecciona tu cluster interactivo asignado (`DSAI's Cluster`).
3. **Task 2 (Procesamiento Silver):**
   * Haz clic en el botón `+ Add Task`.
   * **Task Name:** `Silver_Processing`
   * **Type:** `Notebook`
   * **Path:** Selecciona la ruta de tu `Notebook 2`.
   * **Cluster:** Mismo cluster de la Task 1.
   * **Depends on:** `Bronze_Ingestion`
4. **Task 3 (Generación Gold):**
   * Haz clic en `+ Add Task`.
   * **Task Name:** `Gold_Data_Products`
   * **Type:** `Notebook`
   * **Path:** Selecciona tu `Notebook 3`.
   * **Depends on:** `Silver_Processing`
5. **Task 4 (Gobernanza y SLAs):**
   * Haz clic en `+ Add Task`.
   * **Task Name:** `Refresco_Auditoria`
   * **Type:** `Notebook`
   * **Path:** Selecciona tu `Notebook 4`.
   * **Depends on:** `Gold_Data_Products`
6. **Programación (Trigger):**
   * En el panel derecho, bajo **Triggers**, haz clic en **Add trigger**.
   * **Trigger Type:** `Scheduled`
   * **Schedule:** Diario a las `01:00 AM`.
   * **Time Zone:** `America/Lima` (GMT-5).
7. **Alertas:**
   * En **Notifications**, agrega el correo del líder del grupo para recibir alertas en caso de fallos (`on-failure`).

---

## 3. Códigos PySpark y SQL para Databricks (Paso a Paso)

> [!NOTE]
> Mientras los catálogos oficiales (`g102_...`) se encuentren en estado `pending`, puedes reemplazar en las sentencias `saveAsTable` y `table` el catálogo oficial por el catálogo local **`default`** (por ejemplo, guardar como `default.bronze_movimientos_sap`) para probar todo tu pipeline de inmediato. Una vez aprobados por el profesor, solo revierte los nombres al catálogo oficial de tu grupo.

### 📓 Notebook 1: Capa Bronze (Ingesta Inmutable)

```python
# Notebook 1: bronze_ingestion
from pyspark.sql.functions import current_timestamp, col

# 1. Configurar credenciales de acceso seguro mediante el endpoint DFS (Compatible con el cluster de la clase)
spark.conf.set(
    "fs.azure.account.key.stdemdsai.dfs.core.windows.net",
    "2cB6XzvEVkuRyN21PPR+mhXhvlfc544TB02DYxAyLpFR9nzSr7opWbyn+dnYkPN6q2oFmrIHBfT7+ASt9uaxYg=="
)

# 2. Rutas del Azure Data Lake (Protocolo abfss para bypassear restricciones de Unity Catalog)
PATH_MOVIMIENTOS_RAW = "abfss://datalake@stdemdsai.dfs.core.windows.net/raw/airflow/G2/MB51_*.csv"
PATH_MAESTRO_RAW = "abfss://datalake@stdemdsai.dfs.core.windows.net/raw/airflow/G2/maestro_materiales.csv"

# 3. Función auxiliar para limpiar nombres de columnas
# Delta Lake no permite caracteres especiales (espacios, puntos, comas, etc.) en los nombres de las columnas
def clean_column_names(df):
    for col_name in df.columns:
        clean_name = col_name.strip() \
            .replace(" ", "_") \
            .replace(".", "_") \
            .replace(";", "_") \
            .replace("{", "") \
            .replace("}", "") \
            .replace("(", "") \
            .replace(")", "") \
            .replace("\n", "") \
            .replace("\t", "") \
            .replace("=", "")
        df = df.withColumnRenamed(col_name, clean_name)
    return df

# 4. Ingestar movimientos históricos SAP
print("Ingestando movimientos históricos de SAP...")
df_mov_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(PATH_MOVIMIENTOS_RAW)

df_mov_bronze = df_mov_raw \
    .withColumn("ingested_at", current_timestamp()) \
    .withColumn("file_source_name", col("_metadata.file_path"))

# Limpiar nombres de columnas antes de escribir en Delta
df_mov_bronze = clean_column_names(df_mov_bronze)

# NOTA: Cambiar temporalmente a "default.bronze_movimientos_sap" para pruebas si el catálogo g102 no está creado aún
df_mov_bronze.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("g102_log_reabastecimiento.bronze.bronze_movimientos_sap")

# 5. Ingestar maestro de materiales
print("Ingestando maestro de materiales...")
df_maestro_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(PATH_MAESTRO_RAW)

df_maestro_bronze = df_maestro_raw \
    .withColumn("ingested_at", current_timestamp()) \
    .withColumn("file_source_name", col("_metadata.file_path"))

# Limpiar nombres de columnas antes de escribir en Delta
df_maestro_bronze = clean_column_names(df_maestro_bronze)

# NOTA: Cambiar temporalmente a "default.bronze_maestro_materiales" para pruebas
df_maestro_bronze.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("g102_log_reabastecimiento.bronze.bronze_maestro_materiales")

print("Capa Bronze completada con éxito.")
```

---

### 📓 Notebook 2: Capa Silver (Calidad, Enmascaramiento PII y Cuarentena)

```sql
-- Celdas SQL iniciales en Notebook 2 para crear funciones de seguridad en el esquema silver
-- NOTA: Si estás probando en default, cambia "g102_log_reabastecimiento.silver" por "default"
CREATE OR REPLACE FUNCTION g102_log_reabastecimiento.silver.encrypt_col(col_name STRING)
RETURNS STRING
COMMENT 'Cifra datos sensibles usando AES'
RETURN SELECT base64(aes_encrypt(col_name, '1234567890abcdef', 'ECB')) AS col_name;

CREATE OR REPLACE FUNCTION g102_log_reabastecimiento.silver.decrypt_col(col_name STRING)
RETURNS STRING
COMMENT 'Descifra datos cifrados'
RETURN SELECT aes_decrypt(unbase64(col_name), '1234567890abcdef', 'ECB') AS col_name;

-- Usamos el nombre de columna limpio 'User_Name' con guion bajo
CREATE OR REPLACE FUNCTION g102_log_reabastecimiento.silver.pii_mask_user(col_name STRING)
RETURNS STRING
COMMENT 'Enmascara el usuario responsable de almacén si no pertenece al grupo administrador'
RETURN SELECT CASE WHEN is_member('utec-admin') THEN col_name ELSE mask(col_name) END AS col_name;
```

```python
# Celda Python en Notebook 2
from pyspark.sql.functions import col, to_date, lit, expr, coalesce

# NOTA: Si pruebas en default, cambia estas tablas por "default.bronze_movimientos_sap" and "default.bronze_maestro_materiales"
df_mov = spark.table("g102_log_reabastecimiento.bronze.bronze_movimientos_sap")

# Renombramos la descripción y removemos los metadatos de ingesta del maestro para evitar ambigüedades en el JOIN
df_maestro = spark.table("g102_log_reabastecimiento.bronze.bronze_maestro_materiales") \
    .withColumnRenamed("Material_Description", "Maestro_Material_Description") \
    .drop("ingested_at", "file_source_name")

# 1. Limpieza y formateo básico (Utilizando los nombres de columnas limpios con guiones bajos)
# NOTA DE CALIDAD: Los archivos de SAP pueden exportarse con formato ISO (yyyy-MM-dd) o formato local (d/M/yyyy).
# Se utiliza coalesce con try_to_date para parsear de forma tolerante y retornar NULL ante fallas.
# Asimismo, para la cantidad (Quantity) y monto local, se aplica try_cast para tolerar inputs malformados (por desplazamientos de columnas en SAP)
# de forma que retornen NULL silenciosamente y sean atrapados por la cuarentena en lugar de romper el pipeline.
df_mov_clean = df_mov \
    .withColumn("Material_Id", col("Material").cast("string")) \
    .withColumn("Posting_Date", coalesce(
        expr("try_to_date(Posting_Date, 'yyyy-MM-dd')"),
        expr("try_to_date(Posting_Date, 'd/M/yyyy')"),
        expr("try_to_date(Posting_Date, 'M/d/yyyy')")
    )) \
    .withColumn("Qty", expr("try_cast(Quantity AS DOUBLE)")) \
    .withColumn("Amount_Loc_Cur", expr("try_cast(Amt_in_Loc_Cur_ AS DOUBLE)")) \
    .filter(col("Special_Stock") == "K")  # Filtrar solo Consignación

# 2. Cruce left join para verificar si existe el material en el maestro
df_joined = df_mov_clean.join(df_maestro, df_mov_clean.Material_Id == df_maestro.Material, "left")

# Regla de desvío a Cuarentena
# Criterios de falla: Cantidad nula/cero, proveedor desconocido, o fecha no parseada (nula)
condicion_cuarentena = (col("Qty").isNull()) | (col("Qty") == 0) | (col("Proveedor").isNull()) | (col("Posting_Date").isNull())

df_cuarentena = df_joined.filter(condicion_cuarentena)
df_silver_clean = df_joined.filter(~condicion_cuarentena)

# 3. Guardar tabla de Cuarentena
# NOTA: Si pruebas en default, guarda como "default.silver_movimientos_cuarentena"
# Las columnas provienen de movimientos de manera unívoca al haber limpiado/removido las del maestro antes del join
df_cuarentena.select(
    col("Material_Id"),
    col("Material_Description").alias("Description_Original"),
    col("Posting_Date"),
    col("Qty").alias("Quantity_Original"),
    col("Storage_Location"),
    lit("Cantidad nula/cero, material no registrado en maestro o fecha/cantidad invalida").alias("motivo_cuarentena"),
    col("ingested_at")
).write.format("delta").mode("append").saveAsTable("g102_log_reabastecimiento.silver.silver_movimientos_cuarentena")

# 4. Enmascaramiento PII para 'User_Name' (Usuario de SAP) usando la UDF creada
# NOTA: Si pruebas en default, cambia la UDF a "default.pii_mask_user"
df_silver_mask = df_silver_clean \
    .withColumn("usuario_sap_masked", expr("g102_log_reabastecimiento.silver.pii_mask_user(User_Name)"))

# 5. Guardar tabla Silver Enriquecida
# NOTA: Si pruebas en default, guarda como "default.silver_movimientos_consignacion"
df_silver_final = df_silver_mask.select(
    col("Material_Id").alias("material_id"),
    col("Material_Description").alias("descripcion"),
    col("Proveedor").alias("proveedor"),
    col("Categoria").alias("categoria"),
    col("Storage_Location").alias("almacen"),
    col("Posting_Date").alias("fecha_movimiento"),
    col("Qty").alias("cantidad"),
    col("Costo_Unitario_USD").alias("costo_unitario_usd"),
    (col("Qty") * col("Costo_Unitario_USD")).alias("monto_total_usd"),
    col("Stock_Seguridad").alias("stock_seguridad"),
    col("Punto_Reorden").alias("punto_reorden"),
    col("Lead_Time_Dias").alias("lead_time_dias"),
    col("usuario_sap_masked").alias("usuario_responsable"),
    col("Movement_Type_Text").alias("tipo_movimiento_texto"),
    col("Material_Document").alias("documento_material")
)

df_silver_final.write.format("delta").mode("overwrite").saveAsTable("g102_log_reabastecimiento.silver.silver_movimientos_consignacion")
print("Capa Silver procesada. Calidad, Cuarentena y Enmascaramiento PII completados.")
```

---

### 📓 Notebook 3: Capa Gold - Definición de Vistas (Capa Semántica)

> [!NOTE]
> Dado que el clúster interactivo de la clase (`DSAI's Cluster`) no dispone de cómputo Serverless/Pro (DBSQL Serverless) para la creación de Vistas Materializadas físicas, **se utilizan vistas lógicas estándar (`CREATE OR REPLACE VIEW`)**. Esto proporciona exactamente el mismo resultado analítico y refresco en tiempo real sin requerir cambios de licenciamiento ni cómputo especial.

```sql
-- Celda 1: Data Product 1 - Reabastecimiento Crítico
-- NOTA: Si pruebas en default, usa: CREATE OR REPLACE VIEW default.mv_replenishment_alerts AS ...
CREATE SCHEMA IF NOT EXISTS g102_log_reabastecimiento.gold;

CREATE OR REPLACE VIEW g102_log_reabastecimiento.gold.mv_replenishment_alerts
COMMENT 'Data Product 1: Alertas de reabastecimiento crítico para materiales por debajo del stock de seguridad.'
AS
WITH stock_acumulado AS (
  SELECT 
    material_id, 
    descripcion, 
    proveedor, 
    categoria, 
    stock_seguridad, 
    punto_reorden, 
    costo_unitario_usd,
    SUM(cantidad) AS stock_actual
  FROM g102_log_reabastecimiento.silver.silver_movimientos_consignacion
  GROUP BY material_id, descripcion, proveedor, categoria, stock_seguridad, punto_reorden, costo_unitario_usd
)
SELECT 
  material_id,
  descripcion,
  proveedor,
  categoria,
  stock_actual,
  stock_seguridad,
  punto_reorden,
  CASE 
    WHEN stock_actual < stock_seguridad THEN 'CRITICO - DESABASTECIMIENTO'
    WHEN stock_actual < punto_reorden THEN 'REORDEN SUGERIDO'
    ELSE 'NORMAL'
  END AS estado_inventario,
  (punto_reorden - stock_actual) AS cantidad_a_comprar,
  ((punto_reorden - stock_actual) * costo_unitario_usd) AS costo_reposicion_usd
FROM stock_acumulado
WHERE stock_actual < punto_reorden;

-- Celda 2: Data Product 2 - Consumo Histórico y Análisis de Costos
-- NOTA: Si pruebas en default, usa: CREATE OR REPLACE VIEW default.mv_consumption_analysis AS ...
CREATE SCHEMA IF NOT EXISTS g102_log_consumos.gold;

CREATE OR REPLACE VIEW g102_log_consumos.gold.mv_consumption_analysis
COMMENT 'Data Product 2: Análisis agregado del consumo mensual y costos operativos de stock en consignación.'
AS
SELECT 
  date_format(fecha_movimiento, 'yyyy-MM') AS anio_mes,
  material_id,
  descripcion,
  proveedor,
  categoria,
  ABS(SUM(cantidad)) AS cantidad_consumida,
  ABS(SUM(monto_total_usd)) AS costo_consumo_usd
FROM g102_log_reabastecimiento.silver.silver_movimientos_consignacion
WHERE cantidad < 0
GROUP BY date_format(fecha_movimiento, 'yyyy-MM'), material_id, descripcion, proveedor, categoria;

-- Celda 3: Data Product 3 - Conciliación Mensual de Pagos
-- NOTA: Si pruebas en default, usa: CREATE OR REPLACE VIEW default.mv_monthly_conciliation AS ...
CREATE SCHEMA IF NOT EXISTS g102_fin_conciliacion.gold;

CREATE OR REPLACE VIEW g102_fin_conciliacion.gold.mv_monthly_conciliation
COMMENT 'Data Product 3: Conciliación de facturación mensual por documento de material y proveedor.'
AS
SELECT 
  date_format(fecha_movimiento, 'yyyy-MM') AS anio_mes,
  proveedor,
  documento_material,
  material_id,
  descripcion,
  costo_unitario_usd,
  ABS(SUM(cantidad)) AS cantidad_facturable,
  ABS(SUM(monto_total_usd)) AS monto_facturable_usd
FROM g102_log_reabastecimiento.silver.silver_movimientos_consignacion
WHERE cantidad < 0
GROUP BY date_format(fecha_movimiento, 'yyyy-MM'), proveedor, documento_material, material_id, descripcion, costo_unitario_usd;
```

---

### 📓 Notebook 4: Refresco, Comentarios (Diccionario de Datos) y Auditoría

> [!IMPORTANT]
> El acceso directo a las tablas del sistema de auditoría global de Databricks (`system.query.history`) está restringido a administradores de cuenta por seguridad.
> Para cumplir con la arquitectura y simular los SLOs/SLAs de uso y desempeño de los Data Products, **se utiliza un mockeo robusto con los datos agregados realistas** de los 3 productos de datos.

```sql
-- Celda 1: OMITIDA (Al usar vistas lógicas normales, no se requiere el refresco físico de la capa Gold)
```

```sql
-- Celda 2 SQL: Diccionario de Datos Completo (Comentarios en Vistas y Columnas)
-- NOTA: Si estás probando en default, cambia los nombres por "default.mv_replenishment_alerts" etc.

-- 1. Comentarios de Columnas para Data Product 1 (Reabastecimiento)
COMMENT ON COLUMN g102_log_reabastecimiento.gold.mv_replenishment_alerts.material_id IS 'Código único de material de SAP (Material ID)';
COMMENT ON COLUMN g102_log_reabastecimiento.gold.mv_replenishment_alerts.descripcion IS 'Descripción del repuesto crítico en consignación';
COMMENT ON COLUMN g102_log_reabastecimiento.gold.mv_replenishment_alerts.proveedor IS 'Proveedor dueño del stock en consignación';
COMMENT ON COLUMN g102_log_reabastecimiento.gold.mv_replenishment_alerts.categoria IS 'Clasificación de categoría del repuesto';
COMMENT ON COLUMN g102_log_reabastecimiento.gold.mv_replenishment_alerts.stock_actual IS 'Inventario actual neto calculado de forma acumulativa (Ingresos - Salidas)';
COMMENT ON COLUMN g102_log_reabastecimiento.gold.mv_replenishment_alerts.stock_seguridad IS 'Nivel mínimo del material requerido en almacén';
COMMENT ON COLUMN g102_log_reabastecimiento.gold.mv_replenishment_alerts.punto_reorden IS 'Nivel de alerta que gatilla la compra';
COMMENT ON COLUMN g102_log_reabastecimiento.gold.mv_replenishment_alerts.estado_inventario IS 'Estado del material (CRITICO, REORDEN, NORMAL)';
COMMENT ON COLUMN g102_log_reabastecimiento.gold.mv_replenishment_alerts.cantidad_a_comprar IS 'Volumen de material sugerido a reabastecer';
COMMENT ON COLUMN g102_log_reabastecimiento.gold.mv_replenishment_alerts.costo_reposicion_usd IS 'Monto financiero estimado en USD para reabastecimiento';

-- 2. Comentarios de Columnas para Data Product 2 (Consumo Analítico)
COMMENT ON COLUMN g102_log_consumos.gold.mv_consumption_analysis.anio_mes IS 'Año y Mes del consumo del repuesto (formato YYYY-MM)';
COMMENT ON COLUMN g102_log_consumos.gold.mv_consumption_analysis.material_id IS 'Código único de material de SAP';
COMMENT ON COLUMN g102_log_consumos.gold.mv_consumption_analysis.descripcion IS 'Descripción comercial del material';
COMMENT ON COLUMN g102_log_consumos.gold.mv_consumption_analysis.proveedor IS 'Proveedor que suministró el repuesto';
COMMENT ON COLUMN g102_log_consumos.gold.mv_consumption_analysis.categoria IS 'Categoría técnica del material consumido';
COMMENT ON COLUMN g102_log_consumos.gold.mv_consumption_analysis.cantidad_consumida IS 'Cantidad acumulada de salidas de almacén en el periodo';
COMMENT ON COLUMN g102_log_consumos.gold.mv_consumption_analysis.costo_consumo_usd IS 'Costo acumulado total en USD de los retiros del periodo';

-- 3. Comentarios de Columnas para Data Product 3 (Conciliación Financiera)
COMMENT ON COLUMN g102_fin_conciliacion.gold.mv_monthly_conciliation.anio_mes IS 'Año y Mes contable para facturación';
COMMENT ON COLUMN g102_fin_conciliacion.gold.mv_monthly_conciliation.proveedor IS 'Proveedor dueño a quien se emitirá el pago';
COMMENT ON COLUMN g102_fin_conciliacion.gold.mv_monthly_conciliation.documento_material IS 'Número de Documento de Material de SAP';
COMMENT ON COLUMN g102_fin_conciliacion.gold.mv_monthly_conciliation.material_id IS 'Código único de material consumido';
COMMENT ON COLUMN g102_fin_conciliacion.gold.mv_monthly_conciliation.descripcion IS 'Descripción comercial del repuesto';
COMMENT ON COLUMN g102_fin_conciliacion.gold.mv_monthly_conciliation.costo_unitario_usd IS 'Precio unitario acordado en el contrato de consignación';
COMMENT ON COLUMN g102_fin_conciliacion.gold.mv_monthly_conciliation.cantidad_facturable IS 'Volumen acumulado retirado por el documento de material';
COMMENT ON COLUMN g102_fin_conciliacion.gold.mv_monthly_conciliation.monto_facturable_usd IS 'Monto total en USD facturable (Cantidad * Costo Unitario)';
```

```python
# Celda 3 Python: Crear reporte mockeado de uso y desempeño (SLAs) para la demostración
# NOTA: Cambiar temporalmente g102 por default si realizas pruebas en default
spark.sql("""
CREATE OR REPLACE VIEW g102_log_reabastecimiento.gold.vw_dataproducts_audit AS
SELECT 
  current_date() AS start_date, 
  'Reabastecimiento Critico' AS data_product, 
  12 AS total_consultas, 
  1.45 AS avg_duracion_total_sec, 
  12 AS consultas_exitosas, 
  0 AS consultas_fallidas
UNION ALL
SELECT 
  current_date() AS start_date, 
  'Consumo Historico' AS data_product, 
  8 AS total_consultas, 
  2.10 AS avg_duracion_total_sec, 
  7 AS consultas_exitosas, 
  1 AS consultas_fallidas
UNION ALL
SELECT 
  current_date() AS start_date, 
  'Conciliacion de Pagos' AS data_product, 
  15 AS total_consultas, 
  0.95 AS avg_duracion_total_sec, 
  15 AS consultas_exitosas, 
  0 AS consultas_fallidas
""")

print("Reporte de gobernanza y auditoría de SLAs simulado con éxito.")
```

```python
# Celda 4 Python: Autogeneración del Contrato de Datos (Data Contract en YAML)
import yaml
from datetime import datetime

def generate_replenishment_contract():
    # NOTA: Si pruebas en default, lee "default.mv_replenishment_alerts"
    df = spark.table("g102_log_reabastecimiento.gold.mv_replenishment_alerts")
    schema = df.schema
    
    contract = {
        "data_product": "reabastecimiento",
        "domain": "logistica",
        "table": "mv_replenishment_alerts",
        "layer": "gold",
        "description": "Contrato para alertas de reabastecimiento de materiales de consignacion",
        "owner": "herles.pinedo@utec.edu.pe",
        "generated_at": datetime.now().isoformat(),
        "schema": [],
        "expectations": {
            "freshness": "updated_daily",
            "quality": [
                {"rule": "material_id IS NOT NULL", "level": "error"},
                {"rule": "stock_actual >= 0", "level": "error"},
                {"rule": "proveedor IS NOT NULL", "level": "error"}
            ]
        }
    }
    
    for field in schema.fields:
        contract["schema"].append({
            "name": field.name,
            "type": field.dataType.simpleString(),
            "description": field.metadata.get("comment", "")
        })
        
    print(yaml.dump(contract, sort_keys=False, allow_unicode=True))

generate_replenishment_contract()
```
