# Databricks notebook source
dbutils.widgets.text(
  "volume_path",
  "/Volumes/mingyu_test/default/test2",
  "Path of the volume to be monitored"
)

dbutils.widgets.text(
  "image_prompt_1",
  "describe_the_image",
  "The first prompt for images"
)

dbutils.widgets.text(
  "image_prompt_2",
  "are_there_cats_in_the_image",
  "The second prompt for images"
)

# COMMAND ----------

volume_path = dbutils.widgets.get("volume_path")
image_prompt_1 = dbutils.widgets.get("image_prompt_1")
image_prompt_2 = dbutils.widgets.get("image_prompt_2")

# COMMAND ----------

# DBTITLE 1,Create a volume info table
import requests
import json
# Set the API endpoint URL
host = "https://e2-dogfood.staging.cloud.databricks.com/"
url = host + "api/2.0/unity-catalog/tables"
# Parse the parameter
data = volume_path.split("/")
if len(data) != 5:
    raise Exception("Invalid volume path")
_, _, catalog_name, schema_name, volume_name = data
table_name = f"{volume_name}_info"
get_table_url = f"{url}/{catalog_name}.{schema_name}.{table_name}"

def create_volume_info_table(volume_path: str, image_prompt_1: str, image_prompt_2: str):
#   # Set the request headers
#   token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
#   headers = {
#       "Authorization": f"Bearer {token}",
#       "Content-Type": "application/json"
#   }

#   # Check if the table already exists
#   response = requests.get(get_table_url, headers=headers)
#   if response.status_code == 200:
#       print(f"Table {table_name} already exists")
#       return

#   # Create the table
#   print(f"Creating table for volume: {volume_name}")
#   body = {
#       "catalog_name": catalog_name,
#       "schema_name": schema_name,
#       "name": table_name,
#       "table_type": "MANAGED",
#       "data_source_format": "DELTA",
#       "columns": [
#           {
#               "name": "file_name",
#               "type_name": "string",
#               "type_text": "string",
#               "type_json": "string",
#               "position": 0,
#           },
#           {
#               "name": "file_type",
#               "type_name": "string",
#               "type_text": "string",
#               "type_json": "string",
#               "position": 1,
#           },
#           {
#               "name": "audio_transcriptions",
#               "type_name": "string",
#               "type_text": "string",
#               "type_json": "string",
#               "position": 2,
#           },
#           {
#               "name": image_prompt_1,
#               "type_name": "string",
#               "type_text": "string",
#               "type_json": "string",
#               "position": 3,
#           },
#           {
#               "name": image_prompt_2,
#               "type_name": "string",
#               "type_text": "string",
#               "type_json": "string",
#               "position": 4,
#           },                              
#       ],
#   }
  data = f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.{table_name} (" + \
    "file_name VARCHAR(255)," + \
    "file_type VARCHAR(255)," + \
    "audio_transcriptions VARCHAR(255)," + \
    "audio_summary VARCHAR(255)," + \
    f"{image_prompt_1} VARCHAR(255)," + \
    f"{image_prompt_2} VARCHAR(255)" + \
    ");"
  spark.sql(data)

#   response = requests.post(url, headers=headers, data=json.dumps(body))
#   if response.status_code == 200:
#       print("Table created successfully")
#   else:
#       print(response.status_code)
#       print("Error creating table:", response.content)

# COMMAND ----------

create_volume_info_table(volume_path, image_prompt_1, image_prompt_2)
table = f"{catalog_name}.{schema_name}.{table_name}"
print(table)

# COMMAND ----------

dbutils.notebook.exit(table)

# COMMAND ----------

# DBTITLE 1,List tables in a catalog schema
# For debug purpose
"""
import requests

# Set the API endpoint URL
host = "https://e2-dogfood.staging.cloud.databricks.com/"
url = host + "api/2.0/unity-catalog/tables"

# Set the request headers
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
headers = {
    "Authorization": f"Bearer {token}",
}

# Set the request parameters
params = {
  "catalog_name": "main",
  "schema_name": "shaotong-schema",
}

# Send the API request
response = requests.get(url, headers=headers, params=params)

# Check the response status code
if response.status_code == 200:
    # Print the list of tables
    # print(response.json())
    tables = response.json()["tables"]
    for table in tables:
        print(table["name"])
else:
    print("Error listing tables:", response.content)
"""

# COMMAND ----------

