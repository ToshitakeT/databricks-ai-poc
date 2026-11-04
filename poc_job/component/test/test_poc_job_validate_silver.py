"""
unittest for poc_job_validate_silver component

poc_job_validate_silver ノートブックの data_quality_check 関数を検証するテストコード。
warn / drop / fail の3種類のアクションを検証する。
"""
import unittest

from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType


def data_quality_check(spark, table_name: str, expectations: list) -> dict:
    """Silver層のデータ品質をExpectationsパターンで検証する（ノートブックから複製）"""
    df = spark.read.table(table_name)
    total_count = df.count()

    results = []
    dropped_count = 0
    failed_expectations = []

    for name, condition, action in expectations:
        violations = df.filter(~(condition)).count()
        passed = total_count - violations
        status = "PASS" if violations == 0 else f"{action.upper()}"

        results.append({
            "name": name,
            "action": action,
            "passed": passed,
            "violations": violations,
            "status": status,
        })

        if violations > 0:
            if action == "warn":
                bad_rows = df.filter(~(condition)).select("order_id").limit(5).collect()
                bad_ids = [r["order_id"] for r in bad_rows]
            elif action == "drop":
                before = df.count()
                df = df.filter(condition)
                after = df.count()
                dropped = before - after
                dropped_count += dropped
                total_count = after
            elif action == "fail":
                failed_expectations.append(name)

    if failed_expectations:
        summary = f"データ品質チェック失敗: {failed_expectations}"
        raise RuntimeError(summary)

    final_count = df.count()

    if dropped_count > 0:
        df.createOrReplaceTempView(table_name)

    return {
        "total_input": total_count,
        "total_output": final_count,
        "dropped": dropped_count,
        "expectations": results,
    }


class TestPocJobValidateSilver(unittest.TestCase):
    """poc_job_validate_silver (data_quality_check) のテスト"""

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("test_validate").getOrCreate()
        cls.test_table = "silver_sales_validate_view"

    @classmethod
    def tearDownClass(cls):
        pass

    def _create_test_data(self, rows=None):
        """テスト用Silverデータを作成"""
        if rows is None:
            rows = [
                ("ORD-001", "CUST-101", "Electronics", "129.99", "2025-01-15"),
                ("ORD-002", "CUST-102", "Books",       "15.50",  "2025-01-16"),
                ("ORD-003", "CUST-103", "Electronics", "899.00", "2025-01-17"),
                ("ORD-004", "CUST-101", None,          "59.99",  "2025-01-18"),  # null category
                ("ORD-005", "CUST-104", "Books",       None,     "2025-01-19"),  # null amount
            ]
        df = self.spark.createDataFrame(
            rows,
            schema="order_id STRING, customer_id STRING, product_category STRING, sales_amount STRING, order_date STRING",
        )
        df = df.withColumn("sales_amount", col("sales_amount").cast(DecimalType(10, 2)))
        df.createOrReplaceTempView(self.test_table)

    def test_warn_action_keeps_records(self):
        """warnアクション: 違反レコードがカウントされ、レコードは保持される"""
        self._create_test_data()
        expectations = [
            ("not_null_sales_amount", col("sales_amount").isNotNull(), "warn"),
        ]
        result = data_quality_check(self.spark, self.test_table, expectations)

        self.assertEqual(result["dropped"], 0)
        self.assertEqual(result["total_output"], 5)
        exp = result["expectations"][0]
        self.assertEqual(exp["violations"], 1)
        self.assertEqual(exp["status"], "WARN")

    def test_warn_action_does_not_modify_table(self):
        """warnアクション: テーブルの件数が変更されないことを検証"""
        self._create_test_data()
        expectations = [
            ("not_null_sales_amount", col("sales_amount").isNotNull(), "warn"),
        ]
        data_quality_check(self.spark, self.test_table, expectations)

        table_count = self.spark.read.table(self.test_table).count()
        self.assertEqual(table_count, 5)

    def test_drop_action_removes_records(self):
        """dropアクション: 違反レコードが除外される"""
        self._create_test_data()
        expectations = [
            ("not_null_sales_amount", col("sales_amount").isNotNull(), "drop"),
        ]
        result = data_quality_check(self.spark, self.test_table, expectations)

        self.assertEqual(result["dropped"], 1)
        self.assertEqual(result["total_output"], 4)

    def test_drop_action_updates_table(self):
        """dropアクション: テーブルが更新され、除外後の件数になる"""
        self._create_test_data()
        expectations = [
            ("not_null_sales_amount", col("sales_amount").isNotNull(), "drop"),
        ]
        data_quality_check(self.spark, self.test_table, expectations)

        updated = self.spark.read.table(self.test_table)
        self.assertEqual(updated.count(), 4)

    def test_fail_action_raises_error(self):
        """failアクション: 違反がある場合RuntimeErrorが発生する"""
        self._create_test_data()
        expectations = [
            ("not_null_category", col("product_category").isNotNull(), "fail"),
        ]
        with self.assertRaises(RuntimeError) as ctx:
            data_quality_check(self.spark, self.test_table, expectations)
        self.assertIn("not_null_category", str(ctx.exception))

    def test_fail_action_no_violation_no_error(self):
        """failアクション: 違反が0件の場合エラーが発生しない"""
        self._create_test_data()
        expectations = [
            ("not_null_order_id", col("order_id").isNotNull(), "fail"),
        ]
        result = data_quality_check(self.spark, self.test_table, expectations)
        self.assertEqual(result["dropped"], 0)
        self.assertEqual(result["expectations"][0]["status"], "PASS")

    def test_all_pass_returns_zero_violations(self):
        """全expectation合格時: violations=0, dropped=0"""
        self._create_test_data()
        expectations = [
            ("not_null_order_id", col("order_id").isNotNull(), "warn"),
            ("not_null_customer_id", col("customer_id").isNotNull(), "drop"),
        ]
        result = data_quality_check(self.spark, self.test_table, expectations)

        self.assertEqual(result["dropped"], 0)
        self.assertEqual(result["total_output"], 5)
        for exp in result["expectations"]:
            self.assertEqual(exp["violations"], 0)
            self.assertEqual(exp["status"], "PASS")

    def test_multiple_expectations_combined(self):
        """複数expectation: warn + drop の組み合わせ"""
        self._create_test_data()
        expectations = [
            ("warn_null_category", col("product_category").isNotNull(), "warn"),
            ("drop_null_amount", col("sales_amount").isNotNull(), "drop"),
        ]
        result = data_quality_check(self.spark, self.test_table, expectations)

        self.assertEqual(result["dropped"], 1)
        warn_exp = [e for e in result["expectations"] if e["name"] == "warn_null_category"][0]
        self.assertEqual(warn_exp["violations"], 1)
        self.assertEqual(warn_exp["status"], "WARN")
        drop_exp = [e for e in result["expectations"] if e["name"] == "drop_null_amount"][0]
        self.assertEqual(drop_exp["violations"], 1)
        self.assertEqual(drop_exp["status"], "DROP")

    def test_drop_then_fail_sequence(self):
        """dropで除外された後、failが新しいdfに対して評価される"""
        self._create_test_data()
        expectations = [
            ("drop_null_amount", col("sales_amount").isNotNull(), "drop"),
            ("fail_null_category", col("product_category").isNotNull(), "fail"),
        ]
        with self.assertRaises(RuntimeError):
            data_quality_check(self.spark, self.test_table, expectations)

    def test_return_dict_structure(self):
        """戻り値の辞書が正しいキーを持つことを検証"""
        self._create_test_data()
        expectations = [
            ("not_null_order_id", col("order_id").isNotNull(), "warn"),
        ]
        result = data_quality_check(self.spark, self.test_table, expectations)

        self.assertIn("total_input", result)
        self.assertIn("total_output", result)
        self.assertIn("dropped", result)
        self.assertIn("expectations", result)


if __name__ == "__main__":
    unittest.main()
