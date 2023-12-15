# Databricks notebook source
dbutils.widgets.text(
  "volume_path",
  "/Volumes/<catalog_name>/<schema_name>/<volume_name>",
  "Path of the volume to be monitored"
)

dbutils.widgets.text(
  "image_prompt_1",
  "describe the image",
  "The first prompt for images"
)

dbutils.widgets.text(
  "image_prompt_2",
  "Are there cats in the image? Reply with only a single character 'Y' or 'N'",
  "The second prompt for images"
)

# COMMAND ----------

volume_path = dbutils.widgets.get("volume_path")
image_prompt_1 = dbutils.widgets.get("image_prompt_1")
image_prompt_2 = dbutils.widgets.get("image_prompt_2")

# COMMAND ----------

def create_volume_info_table(volume_path: str, image_prompt_1: str, image_prompt_2: str):
  import requests
  import json

  # Parse the parameter
  data = volume_path.split("/")
  if len(data) != 5:
      raise Exception("Invalid volume path")
  
  _, _, catalog_name, schema_name, volume_name = data
  table_name = f"{volume_name}_info"

  # Set the API endpoint URL
  host = "https://e2-dogfood.staging.cloud.databricks.com/"
  url = host + "api/2.0/unity-catalog/tables"

  # Set the request headers
  token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
  headers = {
      "Authorization": f"Bearer {token}",
      "Content-Type": "application/json"
  }

  # Check if the table already exists
  get_table_url = f"{url}/{catalog_name}.{schema_name}.{table_name}"
  response = requests.get(get_table_url, headers=headers)
  if response.status_code == 200:
      print(f"Table {table_name} already exists")
      return

  # Create the table
  print(f"Creating table for volume: {volume_name}")
  body = {
      "catalog_name": catalog_name,
      "schema_name": schema_name,
      "name": table_name,
      "table_type": "MANAGED",
      "data_source_format": "DELTA",
      "columns": [
          {
              "name": "file_name",
              "type_name": "STRING",
              "type_text": "STRING",
              "type_json": "STRING",
              "position": 0,
          },
          {
              "name": "file_type",
              "type_name": "STRING",
              "type_text": "STRING",
              "type_json": "STRING",
              "position": 1,
          },
          {
              "name": "audio_transcriptions",
              "type_name": "STRING",
              "type_text": "STRING",
              "type_json": "STRING",
              "position": 2,
          },
          {
              "name": image_prompt_1,
              "type_name": "STRING",
              "type_text": "STRING",
              "type_json": "STRING",
              "position": 3,
          },
          {
              "name": image_prompt_2,
              "type_name": "STRING",
              "type_text": "STRING",
              "type_json": "STRING",
              "position": 4,
          },                              
      ],
  }

  response = requests.post(url, headers=headers, data=json.dumps(body))
  if response.status_code == 200:
      print("Table created successfully")
  else:
      print(response.status_code)
      print("Error creating table:", response.content)

# COMMAND ----------

create_volume_info_table(volume_path, image_prompt_1, image_prompt_2)