# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

sample_data = [
    ("ORD-001", "CUST-101", "Electronics",   "129.99", "2025-01-15"),
    ("ORD-002", "CUST-102", "Books",         "15.50",  "2025-01-16"),
    ("ORD-003", "CUST-103", "Electronics",   "899.00", "2025-01-17"),
    ("ORD-004", "CUST-101", "Clothing",      "59.99",  "2025-01-18"),
    ("ORD-005", "CUST-104", "Books",         "32.00",  "2025-01-19"),
    ("ORD-001", "CUST-101", "Electronics",   "129.99", "2025-01-15"),  # 重複行
    ("ORD-006", "CUST-105", "Electronics",   "invalid", "2025-01-20"), # 不正値（クレンジング対象）
    ("ORD-007", "CUST-106", "Clothing",      "79.50",  "2025-01-21"),
    ("ORD-008", "CUST-102", "Books",         "22.75",  "2025-01-22"),
    ("ORD-009", "CUST-103", "Electronics",   "450.00", "2025-01-23"),
    ("ORD-010", "CUST-107", "Books",         "-50.00", "2025-01-24"), # 負値（drop期待のテスト用）
]

# COMMAND ----------

bronze_df = (
    spark.createDataFrame(sample_data,
        schema="order_id STRING, customer_id STRING, product_category STRING, sales_amount STRING, order_date STRING")
    .withColumn("ingested_at", current_timestamp())
)

# Bronze層へ書き込み（上書きモードで冪等性を確保）
bronze_df.write.mode("overwrite").saveAsTable("playground.poc_bronze.bronze_sales")

print(f"Bronze層へ {bronze_df.count()} 件のレコードを挿入しました")
bronze_df.display()