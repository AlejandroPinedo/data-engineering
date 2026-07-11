# Databricks notebook source
# MAGIC %run ./_params

# COMMAND ----------

def generate_dataproduct(domain, dp_name, action='create'):
    data_product = {
        'name': dp_name,
        'catalog_name': f'{domain["code"]}_{dp_name}',
        'description': f'Data product de clientes del dominio de {domain["name"]}'
        }

    if action == 'create':
        spark.sql(f"""CREATE CATALOG IF NOT EXISTS {data_product["catalog_name"]} COMMENT '{data_product["description"]}'""")

        for name_schema, desc_schema in schema_descriptions.items():
            schema_name = f'{data_product["catalog_name"]}.{name_schema}'
            spark.sql(f'CREATE SCHEMA IF NOT EXISTS {schema_name} COMMENT "{desc_schema}"')
            spark.sql(f'ALTER SCHEMA {schema_name} OWNER TO `{admin}`')

        spark.sql(f'ALTER CATALOG {data_product["catalog_name"]} OWNER TO `{admin}`')
    
    elif action == 'delete':
        for name_schema in schema_descriptions.keys():
            schema_name = f'{data_product["catalog_name"]}.{name_schema}'
            spark.sql(f'DROP SCHEMA IF EXISTS {schema_name} CASCADE')
        
        spark.sql(f'DROP SCHEMA IF EXISTS default CASCADE')
        spark.sql(f'DROP CATALOG IF EXISTS {data_product["catalog_name"]} CASCADE')

# COMMAND ----------

domain = {
    'name': 'marketing', # area
    'code':'g2_mkt'
    }

dp_name = 'reviews' # producto

# COMMAND ----------

domain = {
    'name': 'sales',
    'code':'g2_sal'
    }

dp_name = 'orders'

# COMMAND ----------

domain = {
    'name': 'logistic',
    'code':'g2_log'
    }

dp_name = 'stock'

# COMMAND ----------

domain = {
    'name': 'logistic',
    'code':'g2_log'
    }

dp_name = 'product'

# COMMAND ----------

generate_dataproduct(domain, dp_name)

# COMMAND ----------

generate_dataproduct(domain, dp_name, action='delete')

# COMMAND ----------

# MAGIC %md
# MAGIC # Crear Grupos: Reader, Writer, Admin
# MAGIC # Dar Acceso a Grupos al Catalog
# MAGIC # Agregar usuarios al Catalog

# COMMAND ----------

"""
for group, access in privileges.items():
    group_name = f'{data_product["catalog_name"]}_{group}'
    print(group_name)
    print(access)
"""