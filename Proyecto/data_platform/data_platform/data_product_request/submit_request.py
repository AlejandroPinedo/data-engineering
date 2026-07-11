# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Product Request
# MAGIC 1. Fill in all variables in the cell below.
# MAGIC 2. Click **Run All**.
# MAGIC 3. Fix any validation errors and re-run until successful.

# COMMAND ----------

group   = "<your_group_name>"                  # g101–g106 or g201–g206
leader  = "<LEADER_EMAIL>"                     # e.g. juan.perez@utec.edu.pe
members = "<MEMBER1_EMAIL>, <MEMBER2_EMAIL>"   # Comma-separated; do not include the leader

# One tuple per data product: (domain_code, domain_name, dp_name)
# domain_code : exactly 3 lowercase letters        e.g. "cmc"
# domain_name : max 20 chars, meaning of the code  e.g. "comercial"
# dp_name     : max 20 chars, data product name    e.g. "ventas"
data_products = [
    ("<DOMAIN_CODE>", "<DOMAIN_NAME>", "<DATAPRODUCT_NAME>"),
]

# COMMAND ----------

submit_dataproduct_request(group, leader, members, data_products)