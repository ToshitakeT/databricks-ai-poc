"""
unittest for poc_job_update_bronze component

poc_job_update_bronze ノートブックのロジックを検証するテストコード。
サンプルデータの作成、Bronze層テーブルへの書き込み、スキーマ・件数を検証する。
"""
import unittest

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


class TestPocJobUpdateBronze(unittest.TestCase):
    """poc_job_update_bronze のテスト"""

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("test_bronze").getOrCreate()
        cls.spark.sql("CREATE SCHEMA IF NOT EXISTS unittest")
        cls.bronze_table = "unittest.bronze_sales_test"

    @classmethod
    def tearDownClass(cls):
        cls.spark.sql(f"DROP TABLE IF EXISTS {cls.bronze_table}")
        cls.spark.sql("DROP SCHEMA IF EXISTS unittest CASCADE")

    def _create_sample_data(self):
        """ノートブックと同じサンプルデータを生成する"""
        return [
            ("ORD-001", "CUST-101", "Electronics",   "129.99", "2025-01-15"),
            ("ORD-002", "CUST-102", "Books",         "15.50",  "2025-01-16"),
            ("ORD-003", "CUST-103", "Electronics",   "899.00", "2025-01-17"),
            ("ORD-004", "CUST-101", "Clothing",      "59.99",  "2025-01-18"),
            ("ORD-005", "CUST-104", "Books",         "32.00",  "2025-01-19"),
            ("ORD-001", "CUST-101", "Electronics",   "129.99", "2025-01-15"),  # 重複行
            ("ORD-006", "CUST-105", "Electronics",   "invalid", "2025-01-20"), # 不正値
            ("ORD-007", "CUST-106", "Clothing",      "79.50",  "2025-01-21"),
            ("ORD-008", "CUST-102", "Books",         "22.75",  "2025-01-22"),
            ("ORD-009", "CUST-103", "Electronics",   "450.00", "2025-01-23"),
            ("ORD-010", "CUST-107", "Books",         "-50.00", "2025-01-24"), # 負値
        ]

    def _write_bronze_table(self):
        """サンプルデータをBronze層テーブルに書き込む"""
        sample_data = self._create_sample_data()
        bronze_df = (
            self.spark.createDataFrame(
                sample_data,
                schema="order_id STRING, customer_id STRING, product_category STRING, sales_amount STRING, order_date STRING",
            )
            .withColumn("ingested_at", current_timestamp())
        )
        bronze_df.write.mode("overwrite").saveAsTable(self.bronze_table)

    def test_bronze_table_row_count(self):
        """Bronze層のレコード件数が11件であることを検証"""
        self._write_bronze_table()
        result = self.spark.read.table(self.bronze_table)
        self.assertEqual(result.count(), 11)

    def test_bronze_table_schema(self):
        """Bronze層のスキーマが期待通りであることを検証"""
        self._write_bronze_table()
        result = self.spark.read.table(self.bronze_table)
        field_names = [f.name for f in result.schema.fields]
        expected_fields = ["order_id", "customer_id", "product_category",
                           "sales_amount", "order_date", "ingested_at"]
        self.assertEqual(field_names, expected_fields)

    def test_bronze_contains_duplicate(self):
        """重複行 (ORD-001) が2件含まれることを検証"""
        self._write_bronze_table()
        result = self.spark.read.table(self.bronze_table)
        dup_count = result.filter("order_id = 'ORD-001'").count()
        self.assertEqual(dup_count, 2)

    def test_bronze_contains_invalid_sales_amount(self):
        """不正値 'invalid' が sales_amount に含まれることを検証"""
        self._write_bronze_table()
        result = self.spark.read.table(self.bronze_table)
        invalid_count = result.filter("sales_amount = 'invalid'").count()
        self.assertEqual(invalid_count, 1)

    def test_bronze_contains_negative_value(self):
        """負値 '-50.00' が sales_amount に含まれることを検証"""
        self._write_bronze_table()
        result = self.spark.read.table(self.bronze_table)
        neg_count = result.filter("sales_amount = '-50.00'").count()
        self.assertEqual(neg_count, 1)

    def test_bronze_all_columns_are_string(self):
        """sales_amount と order_date が文字列型 (STRING) であることを検証"""
        self._write_bronze_table()
        result = self.spark.read.table(self.bronze_table)
        from pyspark.sql.types import StringType
        schema = {f.name: f.dataType for f in result.schema.fields}
        self.assertIsInstance(schema["sales_amount"], StringType)
        self.assertIsInstance(schema["order_date"], StringType)

    def test_bronze_write_idempotency(self):
        """overwriteモードで2回書き込んでも件数が変わらないことを検証（冪等性）"""
        self._write_bronze_table()
        self._write_bronze_table()  # 2回目
        result = self.spark.read.table(self.bronze_table)
        self.assertEqual(result.count(), 11)


if __name__ == "__main__":
    unittest.main()
