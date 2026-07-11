# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION g0_catalog.default.encrypt_col(col_name STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Función que encrypta datos'
# MAGIC RETURN SELECT base64(aes_encrypt(col_name, '1234567890abcdef', 'ECB')) AS col_name

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION g0_catalog.default.decrypt_col(col_name STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Función que decrypta datos'
# MAGIC RETURN SELECT aes_decrypt(unbase64(col_name), '1234567890abcdef', 'ECB') AS col_name

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION g0_catalog.default.pii_col(col_name STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Función que decrypta si se tiene privilegios'
# MAGIC RETURN SELECT CASE WHEN is_member('utec-admin') THEN g0_catalog.default.decrypt_col(col_name) ELSE col_name END AS col_name

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION g0_catalog.default.pii_mask(col_name STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Función que decrypta si se tiene privilegios'
# MAGIC RETURN SELECT CASE WHEN is_member('utec-admin') THEN col_name ELSE mask(col_name) END AS col_name