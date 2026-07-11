# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT
# MAGIC executed_by, statement_text, execution_status, client_application, total_duration_ms,
# MAGIC start_time, end_time, executed_as
# MAGIC FROM system.query.history
# MAGIC WHERE statement_text like '%g0_mkt_clientes.gold%'

# COMMAND ----------

spark.sql("""
          CREATE OR REPLACE VIEW g0_catalog.default.vw_dp_mkt_reporte AS
          SELECT
            TO_DATE(start_time) AS start_date,
            'g0_mkt_clientes' AS data_product,
            COUNT(*) AS total_queries,
            ROUND(AVG(total_duration_ms)/1000,2) AS avg_total_duration_sec,
            ROUND(AVG(execution_duration_ms)/1000,2) AS avg_exec_duration_sec,
            SUM(CASE WHEN execution_status = 'FINISHED' THEN 1 ELSE 0 END) AS finished_queries,
            SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_queries,
            SUM(CASE WHEN execution_status = 'CANCELED' THEN 1 ELSE 0 END) AS canceled_queries
          FROM system.query.history
          WHERE statement_text LIKE '%g0_mkt_clientes.gold%'
          GROUP BY TO_DATE(start_time)
          """)

# COMMAND ----------

spark.sql("""
          CREATE OR REPLACE VIEW g0_catalog.default.vw_dp_cmc_reporte AS
          SELECT
            TO_DATE(start_time) AS start_date,
            'g0_cmc_ventas' AS data_product,
            COUNT(*) AS total_queries,
            ROUND(AVG(total_duration_ms)/1000,2) AS avg_total_duration_sec,
            ROUND(AVG(execution_duration_ms)/1000,2) AS avg_exec_duration_sec,
            SUM(CASE WHEN execution_status = 'FINISHED' THEN 1 ELSE 0 END) AS finished_queries,
            SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_queries,
            SUM(CASE WHEN execution_status = 'CANCELED' THEN 1 ELSE 0 END) AS canceled_queries
          FROM system.query.history
          WHERE statement_text LIKE '%g0_cmc_ventas.gold%'
          GROUP BY TO_DATE(start_time)
          """)

# COMMAND ----------

spark.sql("""
          CREATE OR REPLACE VIEW g0_catalog.default.vw_dp_ops_reporte AS
          SELECT
            TO_DATE(start_time) AS start_date,
            'g0_ops_prouctos' AS data_product,
            COUNT(*) AS total_queries,
            ROUND(AVG(total_duration_ms)/1000,2) AS avg_total_duration_sec,
            ROUND(AVG(execution_duration_ms)/1000,2) AS avg_exec_duration_sec,
            SUM(CASE WHEN execution_status = 'FINISHED' THEN 1 ELSE 0 END) AS finished_queries,
            SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_queries,
            SUM(CASE WHEN execution_status = 'CANCELED' THEN 1 ELSE 0 END) AS canceled_queries
          FROM system.query.history
          WHERE statement_text LIKE '%g0_ops_prouctos.gold%'
          GROUP BY TO_DATE(start_time)
          """)

# COMMAND ----------

spark.sql("""
          CREATE OR REPLACE VIEW g0_catalog.default.vw_dp_reporte AS
          SELECT * FROM g0_catalog.default.vw_dp_mkt_reporte
          UNION ALL
          SELECT * FROM g0_catalog.default.vw_dp_cmc_reporte
          UNION ALL
          SELECT * FROM g0_catalog.default.vw_dp_ops_reporte
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM g0_catalog.default.vw_dp_reporte