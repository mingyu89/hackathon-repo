# Databricks notebook source
dbutils.widgets.text("image_file_path", "/Volumes/mingyu_test/default/mingyu_test_volumn/Cat_November_2010-1a.jpg")
dbutils.widgets.text("prompt1", "Describe the image")
dbutils.widgets.text("prompt2", "Are there cats in the image? Reply with only a single character 'Y' or 'N'")
image_file_path = dbutils.widgets.get("image_file_path")
prompt1 = dbutils.widgets.get("prompt1")
prompt2 = dbutils.widgets.get("prompt2")

# COMMAND ----------

import requests, json, time

token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
host = "https://e2-dogfood.staging.cloud.databricks.com/"
headers = {
    "Authorization": f"Bearer {token}",
}


# COMMAND ----------

url = host + "serving-endpoints/mingyu-hackathon-vision-endpoint/invocations"

import base64

base64_image = None
with open(image_file_path, "rb") as image_file:
    base64_image = base64.b64encode(image_file.read()).decode("utf-8")


def getData(prompt, image_format, base64_image):
    return {
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"},
                    },
                ],
            }
        ],
    }

# COMMAND ----------

data1 = getData(prompt1, "png", base64_image)
response1 = requests.post(url, headers=headers, json=data1)

# print("Response length: ", len(response1.text))
# print("Response headers: ", response1.headers)
# print(response1.__dict__)
json_response1 = json.loads(response1.text)

# COMMAND ----------

message1 = json_response1["choices"][0]["message"]["content"]
print(message1)

# COMMAND ----------

data2 = getData(prompt2, "png", base64_image)
response2 = requests.post(url, headers=headers, json=data2)
json_response2 = json.loads(response2.text)

# COMMAND ----------

message2 = json_response2["choices"][0]["message"]["content"]
print(message2)

# COMMAND ----------

dbutils.notebook.exit(message1 + ",;,"+  message2)