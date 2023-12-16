# Databricks notebook source
dbutils.widgets.text(
    "audio_file_path",
    "file/path",
    label="Audio File Path",
)

dbutils.widgets.text(
    "open_ai_secret_key_reference",
    "{{secrets/<scope>/<key>}}",
    label="OpenAI Secret Key Reference",
)

dbutils.widgets.text(
    "input_prompt_1",
    "",
    label="First Prompt",
)

dbutils.widgets.text(
    "input_prompt_2",
    "",
    label="Second Prompt",
)

# COMMAND ----------

audio_path = dbutils.widgets.get("audio_file_path")
open_ai_secret_key_reference = dbutils.widgets.get("open_ai_secret_key_reference")
input_prompt_1 = dbutils.widgets.get("input_prompt_1")
input_prompt_2 = dbutils.widgets.get("input_prompt_2")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Open AI Secret Key

# COMMAND ----------

import requests, json, re, binascii

# COMMAND ----------

token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
host = "https://e2-dogfood.staging.cloud.databricks.com/"
headers = {
    "Authorization": f"Bearer {token}",
}


# COMMAND ----------

scope = re.findall("/([^/]+)/", open_ai_secret_key_reference)[0]
key = re.findall("([^/]+)}}", open_ai_secret_key_reference)[0]

# COMMAND ----------


url = host + f"api/2.0/secrets/get?scope={scope}&key={key}"

response = requests.get(url, headers=headers)
open_ai_key = json.loads(response.text)["value"]

# COMMAND ----------

open_ai_key

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convert Audio to Text

# COMMAND ----------

url = "https://api.openai.com/v1/audio/transcriptions"

headers = {"Authorization": f"Bearer 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"}
data = {'model': "whisper-1"}
files = {'file': (audio_path, open(audio_path, 'rb'), 'audio/mpeg')}

response = requests.post(url, headers=headers, data=data, files=files)

# COMMAND ----------

print("Status Code", response.status_code)
print("res", response.text)

audio_text = json.loads(response.text)["text"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Summarize Text

# COMMAND ----------

url = "https://api.openai.com/v1/chat/completions"

headers = {"Authorization": "Bearer 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", 
           "Content-Type": "application/json"}
data = {
  "model": "gpt-3.5-turbo",
  "messages": [
    {
      "role": "user",
      "content": f"Summarize the following text in a couple of sentences: {audio_text}"
    }
  ]
}

response = requests.post(url, headers=headers, json=data)

# COMMAND ----------

choices = json.loads(response.text)["choices"]
audio_summary = choices[0]["message"]["content"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prompt Answers

# COMMAND ----------

def get_prompt_answers(prompt, audio_text):
  url = "https://api.openai.com/v1/chat/completions"

  headers = {"Authorization": "Bearer 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", 
            "Content-Type": "application/json"}
  data = {
    "model": "gpt-3.5-turbo",
    "messages": [
      {
        "role": "user",
        "content": f"Given this text {audio_text}. Answer this question: {prompt}"
      }
    ]
  }

  response = requests.post(url, headers=headers, json=data)

  choices = json.loads(response.text)["choices"]
  answer = choices[0]["message"]["content"]
  return answer

# COMMAND ----------

input_prompt_1_ans = None
input_prompt_2_ans = None

if len(input_prompt_1) > 0:
  input_prompt_1_ans = get_prompt_answers(input_prompt_1, audio_text)

if len(input_prompt_2) > 0:
  input_prompt_2_ans = get_prompt_answers(input_prompt_2, audio_text)

# COMMAND ----------

print(audio_path)
print(audio_text)
print(audio_summary)
print(input_prompt_1_ans)
print(input_prompt_2_ans)

# COMMAND ----------


