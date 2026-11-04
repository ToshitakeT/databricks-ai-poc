"""
unittest for poc_job_update_silver component

poc_job_update_silver ノートブックの bronze_to_silver 関数を検証するテストコード。
型変換、重複排除、クレンジングの動作を検証する。
"""
import unittest

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, current_timestamp, row_number
from pyspark.sql import Window
from pyspark.sql.types import DecimalType, DateType


def bronze_to_silver(spark, bronze_table: str, silver_table: str) -> dict:
    """Bronze層の生データをクレンジングしてSilver層へ書き込む（ノートブックから複製）"""
    bronze_df = spark.read.table(bronze_table)
    bronze_count = bronze_df.count()

    silver_df = (
        bronze_df
        .withColumn("sales_amount",
            when(col("sales_amount").rlike("^\\d+(\\.\\d{1,2})?$"),
                 col("sales_amount").cast(DecimalType(10, 2)))
            .otherwise(None))
        .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
        .withColumn("updated_at", current_timestamp())
        .withColumn("_row_num", row_number().over(
            Window.partitionBy("order_id").orderBy(col("ingested_at").desc())
        ))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
        .select(
            "order_id", "customer_id", "product_category",
            "sales_amount", "order_date", "updated_at"
        )
    )
    silver_count = silver_df.count()

    silver_df.createOrReplaceTempView(silver_table)
    return {"bronze_count": bronze_count, "silver_count": silver_count}


class TestPocJobUpdateSilver(unittest.TestCase):
    """poc_job_update_silver (bronze_to_silver) のテスト"""

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("test_silver").getOrCreate()
        cls.bronze_table = "bronze_sales_silver_view"
        cls.silver_table = "silver_sales_view"

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        """各テスト前にBronze層テストデータを作成"""
        sample_data = [
            ("ORD-001", "CUST-101", "Electronics", "129.99", "2025-01-15"),
            ("ORD-002", "CUST-102", "Books",       "15.50",  "2025-01-16"),
            ("ORD-003", "CUST-103", "Electronics", "899.00", "2025-01-17"),
            ("ORD-001", "CUST-101", "Electronics", "129.99", "2025-01-15"),  # 重複
            ("ORD-006", "CUST-105", "Electronics", "invalid", "2025-01-20"), # 不正値
            ("ORD-010", "CUST-107", "Books",       "-50.00", "2025-01-24"),  # 負値
        ]
        bronze_df = (
            self.spark.createDataFrame(
                sample_data,
                schema="order_id STRING, customer_id STRING, product_category STRING, sales_amount STRING, order_date STRING",
            )
            .withColumn("ingested_at", current_timestamp())
        )
        bronze_df.createOrReplaceTempView(self.bronze_table)

    def test_deduplication_by_order_id(self):
        """重複するORD-001が1件に削減され、全体で5件になることを検証"""
        bronze_to_silver(self.spark, self.bronze_table, self.silver_table)
        result = self.spark.read.table(self.silver_table)
        self.assertEqual(result.count(), 5)  # 6件 - 1重複 = 5件

    def test_sales_amount_type_conversion(self):
        """sales_amountが文字列からDECIMAL(10,2)に変換されることを検証"""
        bronze_to_silver(self.spark, self.bronze_table, self.silver_table)
        result = self.spark.read.table(self.silver_table)
        schema = {f.name: f.dataType for f in result.schema.fields}
        self.assertIsInstance(schema["sales_amount"], DecimalType)
        self.assertEqual(schema["sales_amount"].precision, 10)
        self.assertEqual(schema["sales_amount"].scale, 2)

    def test_invalid_sales_amount_becomes_null(self):
        """不正値 'invalid' がNULLに変換されることを検証"""
        bronze_to_silver(self.spark, self.bronze_table, self.silver_table)
        result = self.spark.read.table(self.silver_table)
        ord006 = result.filter("order_id = 'ORD-006'").collect()[0]
        self.assertIsNone(ord006["sales_amount"])

    def test_negative_sales_amount_becomes_null(self):
        """負値 '-50.00' が正規表現で弾かれNULLに変換されることを検証"""
        bronze_to_silver(self.spark, self.bronze_table, self.silver_table)
        result = self.spark.read.table(self.silver_table)
        ord010 = result.filter("order_id = 'ORD-010'").collect()[0]
        self.assertIsNone(ord010["sales_amount"])

    def test_valid_sales_amount_preserved(self):
        """有効な数値 '129.99' がDECIMALとして保持されることを検証"""
        bronze_to_silver(self.spark, self.bronze_table, self.silver_table)
        result = self.spark.read.table(self.silver_table)
        ord001 = result.filter("order_id = 'ORD-001'").collect()[0]
        self.assertEqual(float(ord001["sales_amount"]), 129.99)

    def test_order_date_type_conversion(self):
        """order_dateが文字列からDATE型に変換されることを検証"""
        bronze_to_silver(self.spark, self.bronze_table, self.silver_table)
        result = self.spark.read.table(self.silver_table)
        schema = {f.name: f.dataType for f in result.schema.fields}
        self.assertIsInstance(schema["order_date"], DateType)

    def test_silver_columns_selected(self):
        """Silver層に必要列のみ選択されることを検証"""
        bronze_to_silver(self.spark, self.bronze_table, self.silver_table)
        result = self.spark.read.table(self.silver_table)
        field_names = set(f.name for f in result.schema.fields)
        expected = {"order_id", "customer_id", "product_category",
                    "sales_amount", "order_date", "updated_at"}
        self.assertEqual(field_names, expected)

    def test_no_ingested_at_in_silver(self):
        """Silver層にingested_at列が含まれないことを検証"""
        bronze_to_silver(self.spark, self.bronze_table, self.silver_table)
        result = self.spark.read.table(self.silver_table)
        field_names = [f.name for f in result.schema.fields]
        self.assertNotIn("ingested_at", field_names)

    def test_updated_at_column_exists(self):
        """updated_at列が存在することを検証"""
        bronze_to_silver(self.spark, self.bronze_table, self.silver_table)
        result = self.spark.read.table(self.silver_table)
        field_names = [f.name for f in result.schema.fields]
        self.assertIn("updated_at", field_names)


if __name__ == "__main__":
    unittest.main()
