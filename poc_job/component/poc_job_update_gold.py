# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql.functions import sum as _sum, count as _count, current_timestamp
from pyspark.sql.types import DecimalType

# COMMAND ----------

# ============================================================
# Component: Silver → Gold 集計
# ============================================================
# 責務: カテゴリ別売上サマリーの作成
# ============================================================
def silver_to_gold(silver_table: str, gold_table: str) -> dict:
    """Silver層のクレンジング済みデータをカテゴリ別に集計してGold層へ書き込む"""
    print(f"[SilverToGold] 読み込み元: {silver_table}")
    silver_df = spark.read.table(silver_table)
    silver_count = silver_df.count()
    print(f"  Silverレコード数: {silver_count}")

    gold_df = (
        silver_df
        .filter(col("sales_amount").isNotNull())  # 不正値を除外
        .groupBy("product_category")
        .agg(
            _sum("sales_amount").cast(DecimalType(12, 2)).alias("total_sales"),
            _count("order_id").alias("order_count"),
        )
        .withColumn("calculated_at", current_timestamp())
        .select("product_category", "total_sales", "order_count", "calculated_at")
    )

    gold_count = gold_df.count()
    print(f"  Goldカテゴリ数: {gold_count}")

    gold_df.write.mode("overwrite").saveAsTable(gold_table)
    print(f"  書き込み先: {gold_table} ✓")
    return {"categories": gold_count, "total_orders": gold_df.agg({"order_count": "sum"}).collect()[0][0]}