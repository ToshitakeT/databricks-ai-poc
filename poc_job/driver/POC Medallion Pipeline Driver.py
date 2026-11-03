# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ../component/poc_job_update_bronze

# COMMAND ----------

# MAGIC %run ../component/poc_job_update_silver

# COMMAND ----------

# MAGIC %run ../component/poc_job_validate_silver

# COMMAND ----------

# MAGIC %run ../component/poc_job_update_gold

# COMMAND ----------

# --- テーブル名 ---
bronze_table = "playground.poc_bronze.bronze_sales"
silver_table = "playground.poc_silver.silver_sales"
gold_table   = "playground.poc_gold.gold_sales_by_category"


# COMMAND ----------

# DBTITLE 1,Silver expectations
from pyspark.sql.functions import col

# --- Silver層のExpectations定義 ---
# (name, condition, action) のタプルリスト
# action: "warn" = 警告のみ / "drop" = レコード除外 / "fail" = パイプライン停止
silver_expectations = [
    ("order_id_not_null",         col("order_id").isNotNull(),          "fail"),
    ("customer_id_not_null",      col("customer_id").isNotNull(),       "fail"),
    ("product_category_not_null", col("product_category").isNotNull(), "fail"),
    ("order_date_not_null",       col("order_date").isNotNull(),        "fail"),
    ("sales_amount_valid",        col("sales_amount").isNotNull(),      "warn"),
    # NULL値はsales_amount_valid(warn)で検出済み。ここでは非NULLの負値のみdrop対象
    ("sales_amount_positive",     col("sales_amount").isNull() | (col("sales_amount") >= 0), "drop"),
]

# COMMAND ----------

# --- パイプライン実行 ---
# Step 1: Bronze → Silver 変換
bronze_to_silver(bronze_table, silver_table)

# Step 2: Silver層のデータ品質チェック (Expectations)
data_quality_check(silver_table, silver_expectations)

# Step 3: Silver → Gold 集計
silver_to_gold(silver_table, gold_table)