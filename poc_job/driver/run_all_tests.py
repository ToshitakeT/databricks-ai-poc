# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,設定
import os
import sys
import unittest

# ノートブックのパスから相対パスでテストディレクトリを特定
# ノートブック: .../poc_job/driver/run_all_tests
# テストディレクトリ: .../poc_job/component/test (= ../component/test)
_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_notebook_dir = os.path.dirname(_notebook_path)  # /Users/.../poc_job/driver
TEST_DIR = os.path.normpath(os.path.join("/Workspace" + _notebook_dir, "..", "component", "test"))

# COMMAND ----------

# DBTITLE 1,全テストファイルを収集
# テストディレクトリ内の test_*.py ファイルをすべて収集
test_files = sorted([
    f for f in os.listdir(TEST_DIR)
    if f.startswith("test_") and f.endswith(".py")
])

print(f"テストディレクトリ: {TEST_DIR}")
print(f"検出されたテストファイル: {len(test_files)}件")
for f in test_files:
    print(f"  - {f}")

# COMMAND ----------

# DBTITLE 1,テスト実行
# テストディレクトリを sys.path に追加して import 可能にする
if TEST_DIR not in sys.path:
    sys.path.insert(0, TEST_DIR)

# unittest の TestLoader でディスカバリ実行
loader = unittest.TestLoader()
suite = loader.discover(TEST_DIR, pattern="test_*.py")

runner = unittest.TextTestRunner(verbosity=2)
result = runner.run(suite)

print(f"\n{'='*60}")
print(f"テスト結果サマリー")
print(f"  実行: {result.testsRun}")
print(f"  成功: {result.testsRun - len(result.failures) - len(result.errors)}")
print(f"  失敗: {len(result.failures)}")
print(f"  エラー: {len(result.errors)}")
print(f"{'='*60}")

if result.wasSuccessful():
    print("✓ 全テストが成功しました")
else:
    print("✗ テストに失敗があります")
    if result.failures:
        print("\n[失敗]")
        for test, traceback in result.failures:
            print(f"  - {test}")
    if result.errors:
        print("\n[エラー]")
        for test, traceback in result.errors:
            print(f"  - {test}")

# COMMAND ----------

# DBTITLE 1,終了コード
# テスト失敗時は例外を発生させてジョブを停止
if not result.wasSuccessful():
    raise RuntimeError(f"unittest失敗: {len(result.failures)} failures, {len(result.errors)} errors")