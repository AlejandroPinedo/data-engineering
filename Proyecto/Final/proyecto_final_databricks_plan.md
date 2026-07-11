# Guía de Implementación Avanzada: Proyecto Final de Ingeniería de Datos
## Medallion Architecture, Data Mesh & Data Governance en Compañía Minera Las Bambas S.A.

Este documento contiene el plan técnico completo y los códigos **PySpark/SQL** para implementar la solución en **Databricks**, adaptando todos los patrones avanzados del profesor en su ejemplo `data_platform` (registro de Data Products, contratos de datos, enmascaramiento PII, vistas materializadas y auditoría de consultas).

---

## 1. Fase 0: Registro de los 3 Data Products en la Plataforma

Para poder disponer de los catálogos en Unity Catalog, primero deben registrar sus 3 Data Products en el administrador ejecutando el siguiente código desde un notebook en Databricks:

* **Dominio Logística (Código: `log`)**: Propietario del abastecimiento y control de inventarios.
* **Dominio Finanzas (Código: `fin`)**: Propietario de la conciliación contable y de pagos.

```python
# Notebook 0: registro_data_products
# MAGIC %run /Shared/data_platform/data_product_request/utils

# CONFIGURA TUS DATOS DE GRUPO UTEC
group = "g202"  # Ajustar a su grupo de sección (ej. g201 - g206)
leader = "[EMAIL_ADDRESS]"  # Correo UTEC del líder
members = "herles.pinedo@utec.edu.pe, david.jimenez@utec.edu.pe, jose.nomberto@utec.edu.pe, kevin.luyo@utec.edu.pe, yessenia.chirinos@utec.edu.pe"

# Solicitud de 3 Data Products
data_products = [
    ("log", "logistica", "reabastecimiento"), # Catálogo resultante: g202_log_reabastecimiento
    ("log", "logistica", "consumos"),         # Catálogo resultante: g202_log_consumos
    ("fin", "finanzas",  "conciliacion")       # Catálogo resultante: g202_fin_conciliacion
]

# Ejecutar validación y envío a la base de datos central de solicitudes
submit_dataproduct_request(group, leader, members, data_products)
```

---

## 2. Arquitectura de Datos y Gobernanza de Seguridad

Para replicar las directivas del profesor, el pipeline incluye:
1. **Seguridad y Enmascaramiento PII:** Mapeo de columnas sensibles. En la data de SAP `MB51`, el campo `User Name` (Usuario del sistema) contiene información sensible. Crearemos funciones de cifrado y una función de máscara PII condicional para que solo el grupo `utec-admin` o administradores de Las Bambas vean el usuario original.
2. **Vistas Materializadas (`MATERIALIZED VIEW`):** Implementadas en la capa Gold para almacenar el resultado físico de las consultas complejas de negocio y actualizarlas de manera programada o a través de comandos `REFRESH`.
3. **Auditoría de Consultas:** Vistas que monitorean la tabla `system.query.history` para auditar el uso de cada Data Product y asegurar los SLAs de disponibilidad y performance.

---

## 3. Códigos PySpark y SQL para Databricks (Paso a Paso)

### 📓 Notebook 1: Capa Bronze (Ingesta Inmutable)

```python
# Notebook 1: bronze_ingestion
from pyspark.sql.functions import current_timestamp, input_file_name

# Rutas de origen (Adaptar según ADLS Gen2 o DBFS)
PATH_MOVIMIENTOS_RAW = "/mnt/raw/inventario_consignacion/MB51_*.csv"
PATH_MAESTRO_RAW = "/mnt/raw/inventario_consignacion/maestro_materiales.csv"

# 1. Ingestar movimientos históricos SAP
df_mov_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(PATH_MOVIMIENTOS_RAW)

df_mov_bronze = df_mov_raw \
    .withColumn("ingested_at", current_timestamp()) \
    .withColumn("file_source_name", input_file_name())

df_mov_bronze.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_movimientos_sap")

# 2. Ingestar maestro de materiales
df_maestro_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(PATH_MAESTRO_RAW)

df_maestro_bronze = df_maestro_raw \
    .withColumn("ingested_at", current_timestamp()) \
    .withColumn("file_source_name", input_file_name())

df_maestro_bronze.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_maestro_materiales")

print("Capa Bronze completada con éxito.")
```

---

### 📓 Notebook 2: Capa Silver (Calidad, Enmascaramiento PII y Cuarentena)

> [!IMPORTANT]
> Primero ejecuta las funciones SQL de enmascaramiento en la celda inicial, y luego corre el código de PySpark para depuración y desvío a cuarentena.

```sql
-- Celdas SQL iniciales en Notebook 2 para crear funciones de seguridad en default
CREATE OR REPLACE FUNCTION default.encrypt_col(col_name STRING)
RETURNS STRING
COMMENT 'Cifra datos sensibles usando AES'
RETURN SELECT base64(aes_encrypt(col_name, '1234567890abcdef', 'ECB')) AS col_name;

CREATE OR REPLACE FUNCTION default.decrypt_col(col_name STRING)
RETURNS STRING
COMMENT 'Descifra datos cifrados'
RETURN SELECT aes_decrypt(unbase64(col_name), '1234567890abcdef', 'ECB') AS col_name;

CREATE OR REPLACE FUNCTION default.pii_mask_user(col_name STRING)
RETURNS STRING
COMMENT 'Enmascara el usuario si no es del grupo administrador'
RETURN SELECT CASE WHEN is_member('utec-admin') THEN col_name ELSE mask(col_name) END AS col_name;
```

```python
# Celda Python en Notebook 2
from pyspark.sql.functions import col, to_date, lit, expr

df_mov = spark.table("bronze_movimientos_sap")
df_maestro = spark.table("bronze_maestro_materiales")

# 1. Limpieza y formateo básico
df_mov_clean = df_mov \
    .withColumn("Material_Id", col("Material").cast("string")) \
    .withColumn("Posting_Date", to_date(col("Posting Date"), "yyyy-MM-dd")) \
    .withColumn("Qty", col("Quantity").cast("double")) \
    .withColumn("Amount_Loc_Cur", col("Amt.in Loc.Cur.").cast("double")) \
    .filter(col("Special Stock") == "K")  # Filtrar solo Consignación

# 2. Cruce left join para verificar si existe el material en el maestro
df_joined = df_mov_clean.join(df_maestro, df_mov_clean.Material_Id == df_maestro.Material, "left")

# Regla de desvío a Cuarentena
condicion_cuarentena = (col("Qty").isNull()) | (col("Qty") == 0) | (col("Proveedor").isNull())

df_cuarentena = df_joined.filter(condicion_cuarentena)
df_silver_clean = df_joined.filter(~condicion_cuarentena)

# 3. Guardar tabla de Cuarentena
df_cuarentena.select(
    col("Material_Id"),
    col("Material Description").alias("Description_Original"),
    col("Posting_Date"),
    col("Qty").alias("Quantity_Original"),
    col("Storage Location"),
    lit("Cantidad nula/cero o material no registrado en maestro").alias("motivo_cuarentena"),
    col("ingested_at")
).write.format("delta").mode("append").saveAsTable("silver_movimientos_cuarentena")

# 4. Enmascaramiento PII para 'User Name' (Usuario de SAP) usando la UDF creada
df_silver_mask = df_silver_clean \
    .withColumn("usuario_sap_masked", expr("default.pii_mask_user(`User Name`)"))

# 5. Guardar tabla Silver Enriquecida
df_silver_final = df_silver_mask.select(
    col("Material_Id").alias("material_id"),
    col("Material_Description").alias("descripcion"),
    col("Proveedor").alias("proveedor"),
    col("Categoria").alias("categoria"),
    col("Storage Location").alias("almacen"),
    col("Posting_Date").alias("fecha_movimiento"),
    col("Qty").alias("cantidad"),
    col("Costo_Unitario_USD").alias("costo_unitario_usd"),
    (col("Qty") * col("Costo_Unitario_USD")).alias("monto_total_usd"),
    col("Stock_Seguridad").alias("stock_seguridad"),
    col("Punto_Reorden").alias("punto_reorden"),
    col("Lead_Time_Dias").alias("lead_time_dias"),
    col("usuario_sap_masked").alias("usuario_responsable"),
    col("Movement Type Text").alias("tipo_movimiento_texto"),
    col("Material Document").alias("documento_material")
)

df_silver_final.write.format("delta").mode("overwrite").saveAsTable("silver_movimientos_consignacion")
print("Capa Silver procesada. Calidad, Cuarentena y Enmascaramiento PII completados.")
```

---

### 📓 Notebook 3: Capa Gold - Definición de Vistas Materializadas

> [!TIP]
> Dado que usaremos el Catálogo Federado provisto por la solicitud, utilizaremos sentencias SQL para crear **Vistas Materializadas** (`MATERIALIZED VIEW`) en cada uno de los 3 catálogos creados. Reemplaza `g202` por el número de tu grupo real.

```sql
-- Celda 1: Data Product 1 - Reabastecimiento Crítico
-- Ubicado en el catálogo: g202_log_reabastecimiento.gold
CREATE SCHEMA IF NOT EXISTS g202_log_reabastecimiento.gold;

CREATE OR REPLACE MATERIALIZED VIEW g202_log_reabastecimiento.gold.mv_replenishment_alerts
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
  FROM default.silver_movimientos_consignacion
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
-- Ubicado en el catálogo: g202_log_consumos.gold
CREATE SCHEMA IF NOT EXISTS g202_log_consumos.gold;

CREATE OR REPLACE MATERIALIZED VIEW g202_log_consumos.gold.mv_consumption_analysis
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
FROM default.silver_movimientos_consignacion
WHERE cantidad < 0
GROUP BY date_format(fecha_movimiento, 'yyyy-MM'), material_id, descripcion, proveedor, categoria;

-- Celda 3: Data Product 3 - Conciliación Mensual de Pagos
-- Ubicado en el catálogo: g202_fin_conciliacion.gold
CREATE SCHEMA IF NOT EXISTS g202_fin_conciliacion.gold;

CREATE OR REPLACE MATERIALIZED VIEW g202_fin_conciliacion.gold.mv_monthly_conciliation
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
FROM default.silver_movimientos_consignacion
WHERE cantidad < 0
GROUP BY date_format(fecha_movimiento, 'yyyy-MM'), proveedor, documento_material, material_id, descripcion, costo_unitario_usd;
```

---

### 📓 Notebook 4: Refresco y Auditoría del Data Product (Queries History)

```sql
-- Celda 1: Comando de refresco manual para las Vistas Materializadas
REFRESH MATERIALIZED VIEW g202_log_reabastecimiento.gold.mv_replenishment_alerts;
REFRESH MATERIALIZED VIEW g202_log_consumos.gold.mv_consumption_analysis;
REFRESH MATERIALIZED VIEW g202_fin_conciliacion.gold.mv_monthly_conciliation;

-- Celda 2: Crear vistas de control de auditoría en default usando query.history
-- Esto audita el rendimiento y uso de los Data Products del grupo
CREATE OR REPLACE VIEW default.vw_dataproducts_audit AS
SELECT
  TO_DATE(start_time) AS start_date,
  CASE 
    WHEN statement_text LIKE '%g202_log_reabastecimiento%' THEN 'Reabastecimiento Critico'
    WHEN statement_text LIKE '%g202_log_consumos%' THEN 'Consumo Historico'
    WHEN statement_text LIKE '%g202_fin_conciliacion%' THEN 'Conciliacion de Pagos'
    ELSE 'Otros'
  END AS data_product,
  COUNT(*) AS total_consultas,
  ROUND(AVG(total_duration_ms)/1000,2) AS avg_duracion_total_sec,
  SUM(CASE WHEN execution_status = 'FINISHED' THEN 1 ELSE 0 END) AS consultas_exitosas,
  SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) AS consultas_fallidas
FROM system.query.history
WHERE statement_text LIKE '%g202_%'
GROUP BY TO_DATE(start_time), 
  CASE 
    WHEN statement_text LIKE '%g202_log_reabastecimiento%' THEN 'Reabastecimiento Critico'
    WHEN statement_text LIKE '%g202_log_consumos%' THEN 'Consumo Historico'
    WHEN statement_text LIKE '%g202_fin_conciliacion%' THEN 'Conciliacion de Pagos'
    ELSE 'Otros'
  END;

-- Visualizar uso de los Data Products
SELECT * FROM default.vw_dataproducts_audit ORDER BY start_date DESC;
```

---

## 4. Contrato de Datos (Data Contract en YAML)

De acuerdo con el enfoque de gobernanza del profesor, a continuación tienes el código para generar el Contrato de Datos YAML de tu tabla Gold de reabastecimiento:

```python
# Celda Python en Notebook 4
import yaml
from datetime import datetime

def generate_replenishment_contract():
    df = spark.table("g202_log_reabastecimiento.gold.mv_replenishment_alerts")
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
