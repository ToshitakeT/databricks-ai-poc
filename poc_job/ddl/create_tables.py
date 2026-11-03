# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %sql
# MAGIC USE CATALOG playground;
# MAGIC
# MAGIC -- 【Bronze層】生データ（原則として文字列やタイムスタンプでそのまま保持）
# MAGIC CREATE TABLE poc_bronze.bronze_sales (
# MAGIC     order_id STRING COMMENT '注文ID',
# MAGIC     customer_id STRING COMMENT '顧客ID',
# MAGIC     product_category STRING COMMENT '商品カテゴリ',
# MAGIC     sales_amount STRING COMMENT '売上金額（生データのため文字列）',
# MAGIC     order_date STRING COMMENT '注文日（生データのため文字列）',
# MAGIC     ingested_at TIMESTAMP COMMENT '取り込み日時'
# MAGIC   ) USING DELTA
# MAGIC   COMMENT '生データをそのまま格納するブロンズ層';
# MAGIC
# MAGIC -- 【Silver層】クレンジング済みデータ（型変換や重複排除を実施）
# MAGIC CREATE TABLE poc_silver.silver_sales (
# MAGIC     order_id STRING COMMENT '注文ID',
# MAGIC     customer_id STRING COMMENT '顧客ID',
# MAGIC     product_category STRING COMMENT '商品カテゴリ',
# MAGIC     sales_amount DECIMAL(10, 2) COMMENT '売上金額',
# MAGIC     order_date DATE COMMENT '注文日',
# MAGIC     updated_at TIMESTAMP COMMENT '更新日時'
# MAGIC   ) USING DELTA
# MAGIC   COMMENT 'クレンジング・型変換済みのシルバー層';
# MAGIC
# MAGIC -- 【Gold層】ビジネス集計用データ（カテゴリ別の売上サマリーなど）
# MAGIC CREATE TABLE poc_gold.gold_sales_by_category (
# MAGIC     product_category STRING COMMENT '商品カテゴリ',
# MAGIC     total_sales DECIMAL(12, 2) COMMENT '総売上金額',
# MAGIC     order_count LONG COMMENT '注文数',
# MAGIC     calculated_at TIMESTAMP COMMENT '集計日時'
# MAGIC   ) USING DELTA
# MAGIC   COMMENT '分析・ダッシュボード向けのゴールド層（カテゴリ別集計）';