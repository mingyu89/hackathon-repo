# Databricks notebook source
dbutils.widgets.text("volume_path", "/Volumes/mingyu_test/default/test")
volume_path = dbutils.widgets.get("volume_path")

# COMMAND ----------

# create table 
dbutils.notebook.run("create_uc_table", 1000000, {})

# COMMAND ----------

# create endpoint
dbutils.notebook.run("create_external_model_endpoint", 1000000, {})

# COMMAND ----------

# create endpoint
dbutils.notebook.run("list_compare_files_add_to_table", 1000000, {})