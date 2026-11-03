# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# ============================================================
# Component: Silver層データ品質チェック (Expectations)
# ============================================================
# 責務: Silver層データに対するデータ品質ルールの評価
#       SDP expectations と同様に warn / drop / fail の3種類をサポート
# ============================================================
def data_quality_check(table_name: str, expectations: list) -> dict:
    """Silver層のデータ品質をExpectationsパターンで検証する

    Expectations定義の形式:
        (name, condition, action)
        - action="warn": 違反レコードを警告ログに出力（レコードは保持）
        - action="drop": 違反レコードを除外
        - action="fail": 違反が1件でもあればパイプラインを即座に停止
    """
    print(f"[DataQuality] 対象テーブル: {table_name}")
    df = spark.read.table(table_name)
    total_count = df.count()
    print(f"  レコード数: {total_count}")

    results = []
    dropped_count = 0
    failed_expectations = []

    for name, condition, action in expectations:
        # 違反レコード数をカウント
        violations = df.filter(~(condition)).count()
        passed = total_count - violations
        status = "PASS" if violations == 0 else f"{action.upper()}"

        print(f"  [{action.upper():4s}] {name}: {passed} passed, {violations} violations")

        results.append({
            "name": name,
            "action": action,
            "passed": passed,
            "violations": violations,
            "status": status,
        })

        if violations > 0:
            if action == "warn":
                # 警告のみ（レコードは保持）
                bad_rows = df.filter(~(condition)).select("order_id").limit(5).collect()
                bad_ids = [r["order_id"] for r in bad_rows]
                print(f"    warn 件数: {violations} (例: {bad_ids})")

            elif action == "drop":
                # 違反レコードを除外
                before = df.count()
                df = df.filter(condition)
                after = df.count()
                dropped = before - after
                dropped_count += dropped
                total_count = after  # drop後にtotal_countを更新
                print(f"    drop 件数: {dropped}")

            elif action == "fail":
                # 即座にパイプライン停止
                bad_rows = df.filter(~(condition)).select("order_id").limit(5).collect()
                bad_ids = [r["order_id"] for r in bad_rows]
                failed_expectations.append(name)
                print(f"    FAIL 件数: {violations} (例: {bad_ids})")

    if failed_expectations:
        summary = f"データ品質チェック失敗: {failed_expectations}"
        print(f"  ✗ データ品質エラーでパイプラインを停止します")
        raise RuntimeError(summary)

    final_count = df.count()

    # drop期待でレコードが除外された場合はテーブルを更新
    if dropped_count > 0:
        df.write.mode("overwrite").saveAsTable(table_name)
        print(f"  テーブル更新: {table_name} (drop除外: {dropped_count}件) ✓")

    print(f"  最終レコード数: {final_count} (drop除外: {dropped_count})")
    print(f"  ✓ データ品質チェック完了")

    return {
        "total_input": total_count,
        "total_output": final_count,
        "dropped": dropped_count,
        "expectations": results,
    }