# Databricks notebook source
import yaml
from typing import List
from pyspark.sql import SparkSession
from datetime import datetime

# COMMAND ----------

def get_data_contract(
    catalog_name:str = "g0_mkt_clientes",
    schema_name:str = "gold",
    table_name:str = "vw_clientes",
    data_product_name:str = "clientes",
    domain_name:str = "marketing",
    owner_email:str = "angel.tintaya@utec.edu.pe",
    freshness: str = "updated_daily",
    rules_field_not_null: List[str] = None,
    rules_field_email: List[str] = None,
    rules_field_dob: List[str] = None
    ) -> str:
    """
    Genera un contrato de datos (data contract) en formato YAML para una tabla Gold 
    de un Data Product en una arquitectura Mesh Medallion en Databricks.

    Parameters
    ----------
    catalog_name : str
        Nombre del catálogo donde se encuentra la tabla (equivale al data product).
    schema_name : str
        Nombre del esquema (layer Medallion: bronze, silver, gold).
    table_name : str
        Nombre de la tabla para la cual se genera el contrato.
    data_product_name : str
        Nombre del Data Product.
    domain_name : str
        Nombre del dominio de negocio (por ejemplo, 'marketing', 'comercial').
    owner_email : str
        Correo del responsable del Data Product.
    freshness : str
            Frecuencia esperada de actualización de los datos. Valores válidos:
            ['updated_daily', 'updated_hourly', 'updated_weekly', 'updated_monthly', 'real_time', 'static']
    rules_field_not_null : List[str], optional
        Lista de campos que no deben contener nulos (nivel de error).
    rules_field_email : List[str], optional
        Lista de campos que deben cumplir formato de email (nivel de warning).
    rules_field_dob : List[str], optional
        Lista de campos que deben ser fechas válidas de nacimiento (nivel de warning).

    Returns
    -------
    str
        Contrato de datos en formato YAML.
    """
    valid_freshness_values = [
        "updated_daily", "updated_hourly", "updated_weekly", "updated_monthly", "real_time", "static"
    ]
    if freshness not in valid_freshness_values:
        raise ValueError(
            f"El valor de 'freshness' debe ser uno de: {valid_freshness_values}"
        )

    rules_field_not_null = rules_field_not_null or []
    rules_field_email = rules_field_email or []
    rules_field_dob = rules_field_dob or []

    df = spark.table(f"{catalog_name}.{schema_name}.{table_name}")
    schema = df.schema

    contract = {
        "data_product": data_product_name,
        "domain": domain_name,
        "table": table_name,
        "layer": schema_name,
        "description": f"Contrato generado automáticamente para {table_name}",
        "owner": owner_email,
        "generated_at": datetime.now().isoformat(),
        "schema": [],
        "expectations": {
            "freshness": freshness,
            "quality": []
        }
    }

    for field in schema.fields:
        contract["schema"].append({
            "name": field.name,
            "type": field.dataType.simpleString(),
            "description": field.metadata.get("comment", "")
        })
        
        if field.name in rules_field_not_null:
            contract["expectations"]["quality"].append({
                "rule": f"{field.name} IS NOT NULL",
                "level": "error"
            })
        if field.name in rules_field_email:
            contract["expectations"]["quality"].append({
                "rule": f"{field.name} LIKE '%@%'",
                "level": "warning"
            })
        if field.name in rules_field_dob:
            contract["expectations"]["quality"].append({
                "rule": f"{field.name} < current_date()",
                "level": "warning"
            })
    
    return yaml.dump(contract, sort_keys=False, allow_unicode=True)