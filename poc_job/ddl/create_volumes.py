# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %sql
# MAGIC USE CATALOG playground;
# MAGIC CREATE VOLUME poc_bronze.file_input;