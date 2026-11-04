"""
unittest for poc_job_update_gold component

poc_job_update_gold ノートブックの silver_to_gold 関数を検証するテストコード。
カテゴリ別売上集計の動作を検証する。
"""
import unittest

import datetime
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count as _count, current_timestamp
from pyspark.sql.types import DecimalType


def silver_to_gold(spark, silver_table: str, gold_table: str) -> dict:
    """Silver層のクレンジング済みデータをカテゴリ別に集計してGold層へ書き込む（ノートブックから複製）"""
    silver_df = spark.read.table(silver_table)
    silver_count = silver_df.count()

    gold_df = (
        silver_df
        .filter(col("sales_amount").isNotNull())
        .groupBy("product_category")
        .agg(
            _sum("sales_amount").cast(DecimalType(12, 2)).alias("total_sales"),
            _count("order_id").alias("order_count"),
        )
        .withColumn("calculated_at", current_timestamp())
        .select("product_category", "total_sales", "order_count", "calculated_at")
    )

    gold_count = gold_df.count()

    gold_df.createOrReplaceTempView(gold_table)
    return {
        "categories": gold_count,
        "total_orders": gold_df.agg({"order_count": "sum"}).collect()[0][0],
    }


class TestPocJobUpdateGold(unittest.TestCase):
    """poc_job_update_gold (silver_to_gold) のテスト"""

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("test_gold").getOrCreate()
        cls.silver_table = "silver_sales_gold_view"
        cls.gold_table = "gold_sales_view"

    @classmethod
    def tearDownClass(cls):
        pass

    def _create_silver_data(self, rows=None):
        """テスト用Silverデータを作成"""
        if rows is None:
            rows = [
                ("ORD-001", "CUST-101", "Electronics", "129.99", datetime.date(2025, 1, 15)),
                ("ORD-002", "CUST-102", "Books",       "15.50",  datetime.date(2025, 1, 16)),
                ("ORD-003", "CUST-103", "Electronics", "899.00", datetime.date(2025, 1, 17)),
                ("ORD-004", "CUST-101", "Clothing",    "59.99",  datetime.date(2025, 1, 18)),
                ("ORD-005", "CUST-104", "Books",       "32.00",  datetime.date(2025, 1, 19)),
                ("ORD-006", "CUST-105", "Electronics", None,     datetime.date(2025, 1, 20)), # null amount
            ]
        df = self.spark.createDataFrame(
            rows,
            schema="order_id STRING, customer_id STRING, product_category STRING, sales_amount STRING, order_date DATE",
        )
        df = df.withColumn("sales_amount", col("sales_amount").cast(DecimalType(10, 2)))
        df.createOrReplaceTempView(self.silver_table)

    def test_gold_category_count(self):
        """Gold層のカテゴリ数が3 (Electronics, Books, Clothing) であることを検証"""
        self._create_silver_data()
        silver_to_gold(self.spark, self.silver_table, self.gold_table)
        result = self.spark.read.table(self.gold_table)
        self.assertEqual(result.count(), 3)

    def test_gold_excludes_null_sales_amount(self):
        """sales_amountがNULLのレコードが集計から除外されることを検証"""
        self._create_silver_data()
        silver_to_gold(self.spark, self.silver_table, self.gold_table)
        result = self.spark.read.table(self.gold_table)
        total_orders = result.agg({"order_count": "sum"}).collect()[0][0]
        self.assertEqual(total_orders, 5)  # 6件中1件null → 5件集計

    def test_gold_electronics_total_sales(self):
        """Electronicsの合計売上が 129.99 + 899.00 = 1028.99 であることを検証"""
        self._create_silver_data()
        silver_to_gold(self.spark, self.silver_table, self.gold_table)
        result = self.spark.read.table(self.gold_table)
        electronics = result.filter("product_category = 'Electronics'").collect()[0]
        self.assertEqual(float(electronics["total_sales"]), 1028.99)

    def test_gold_books_total_sales(self):
        """Booksの合計売上が 15.50 + 32.00 = 47.50 であることを検証"""
        self._create_silver_data()
        silver_to_gold(self.spark, self.silver_table, self.gold_table)
        result = self.spark.read.table(self.gold_table)
        books = result.filter("product_category = 'Books'").collect()[0]
        self.assertEqual(float(books["total_sales"]), 47.50)

    def test_gold_clothing_total_sales(self):
        """Clothingの合計売上が 59.99 であることを検証"""
        self._create_silver_data()
        silver_to_gold(self.spark, self.silver_table, self.gold_table)
        result = self.spark.read.table(self.gold_table)
        clothing = result.filter("product_category = 'Clothing'").collect()[0]
        self.assertEqual(float(clothing["total_sales"]), 59.99)

    def test_gold_electronics_order_count(self):
        """Electronicsの注文件数が2件であることを検証"""
        self._create_silver_data()
        silver_to_gold(self.spark, self.silver_table, self.gold_table)
        result = self.spark.read.table(self.gold_table)
        electronics = result.filter("product_category = 'Electronics'").collect()[0]
        self.assertEqual(electronics["order_count"], 2)

    def test_gold_schema(self):
        """Gold層のスキーマが期待通りであることを検証"""
        self._create_silver_data()
        silver_to_gold(self.spark, self.silver_table, self.gold_table)
        result = self.spark.read.table(self.gold_table)
        field_names = [f.name for f in result.schema.fields]
        expected = ["product_category", "total_sales", "order_count", "calculated_at"]
        self.assertEqual(field_names, expected)

    def test_gold_total_sales_type(self):
        """total_salesがDECIMAL(12,2)型であることを検証"""
        self._create_silver_data()
        silver_to_gold(self.spark, self.silver_table, self.gold_table)
        result = self.spark.read.table(self.gold_table)
        total_sales_type = {f.name: f.dataType for f in result.schema.fields}["total_sales"]
        self.assertEqual(str(total_sales_type), "DecimalType(12,2)")

    def test_gold_return_value(self):
        """戻り値の辞書が正しい categories と total_orders を持つことを検証"""
        self._create_silver_data()
        result = silver_to_gold(self.spark, self.silver_table, self.gold_table)
        self.assertEqual(result["categories"], 3)
        self.assertEqual(result["total_orders"], 5)

    def test_gold_empty_input(self):
        """入力Silver層が空の場合、Gold層も0件になることを検証"""
        self._create_silver_data(rows=[])
        silver_to_gold(self.spark, self.silver_table, self.gold_table)
        result = self.spark.read.table(self.gold_table)
        self.assertEqual(result.count(), 0)

    def test_gold_all_null_amounts(self):
        """全レコードのsales_amountがNULLの場合、Gold層が0件になることを検証"""
        rows = [
            ("ORD-001", "CUST-101", "Electronics", None, datetime.date(2025, 1, 15)),
            ("ORD-002", "CUST-102", "Books",       None, datetime.date(2025, 1, 16)),
        ]
        self._create_silver_data(rows=rows)
        silver_to_gold(self.spark, self.silver_table, self.gold_table)
        result = self.spark.read.table(self.gold_table)
        self.assertEqual(result.count(), 0)


if __name__ == "__main__":
    unittest.main()
