# Databricks notebook source
dbutils.widgets.text("volume_path", "/Volumes/mingyu_test/default/test2")
volume_path = dbutils.widgets.get("volume_path")
dbutils.widgets.text("table_path", "mingyu_test.default.test2_info")
table_path = dbutils.widgets.get("table_path")

# COMMAND ----------

# List volume files
files = dbutils.fs.ls(volume_path)
files_path = []
for file in files:
  files_path.append(file.path)
print(files_path)

# COMMAND ----------

# List table rows
# query = f"SELECT * FROM {table_path}"
# print(query)

# Run the query and get a DataFrame
df = spark.sql(f"select * from {table_path}")
first_column = df.select(df.columns[0])
first_column_list = [row[0] for row in first_column.collect()]
print(first_column_list)

# COMMAND ----------



for path in files_path:
  file_name = path.split("/")[-1]
  if not file_name in first_column_list:
    extension = file_name.split(".")[-1]
    if extension == "jpg" or  extension == "png":
      print("pic "+ file_name)
      # image
      t = dbutils.notebook.run("get_image_messages", 1000000, {"image_file_path": path[5:]})
      print(t)
      messages = t.split(',;,')
      print(messages)
      t1 = messages[0].replace('"', '') 
      df = spark.sql(f'INSERT INTO {table_path} (file_name, file_type, audio_transcriptions, audio_summary, describe_the_image, are_there_cats_in_the_image) VALUES ("{file_name}", "image", " ", " ", "{t1}", "{messages[1]}")')

    elif extension == "m4a":
      print("audio "+ file_name)
      # audio 
      t = dbutils.notebook.run("get_audio_messages", 1000000, {"audio_file_path": path[5:]})
      print(t)
      messages = t.split(',;,')
      print(messages)
      df = spark.sql(f'INSERT INTO {table_path} (file_name, file_type, audio_transcriptions, audio_summary, describe_the_image, are_there_cats_in_the_image) VALUES ("{file_name}", "audio", "{messages[0]}", "{messages[1]}", " " ," " )')
    else:
      print("unknown file "+file_name)