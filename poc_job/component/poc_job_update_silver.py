# Databricks notebook source
from pyspark.sql.functions import col, to_date, when, current_timestamp, row_number
from pyspark.sql import Window
from pyspark.sql.types import DecimalType

# COMMAND ----------

# ============================================================
# Component: Bronze → Silver 変換
# ============================================================
# 責務: 型変換、重複排除、クレンジング
# ============================================================

def bronze_to_silver(bronze_table: str, silver_table: str) -> dict:
    """Bronze層の生データをクレンジングしてSilver層へ書き込む"""
    print(f"[BronzeToSilver] 読み込み元: {bronze_table}")
    bronze_df = spark.read.table(bronze_table)
    bronze_count = bronze_df.count()
    print(f"  Bronzeレコード数: {bronze_count}")

    silver_df = (
        bronze_df
        # sales_amount: 文字列 → DECIMAL(10,2)（不正値はNULLに変換）
        .withColumn("sales_amount",
            when(col("sales_amount").rlike("^\\d+(\\.\\d{1,2})?$"),
                 col("sales_amount").cast(DecimalType(10, 2)))
            .otherwise(None))
        # order_date: 文字列 → DATE
        .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
        # 更新日時を付与
        .withColumn("updated_at", current_timestamp())
        # 重複排除（order_idが主キー、最新ingested_atを優先して決定的に保持）
        .withColumn("_row_num", row_number().over(
            Window.partitionBy("order_id").orderBy(col("ingested_at").desc())
        ))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
        # 必要列のみ選択
        .select(
            "order_id", "customer_id", "product_category",
            "sales_amount", "order_date", "updated_at"
        )
    )

    silver_count = silver_df.count()

    silver_df.write.mode("overwrite").saveAsTable(silver_table)