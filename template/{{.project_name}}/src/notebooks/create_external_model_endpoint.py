# Databricks notebook source
dbutils.widgets.text(
    "open_ai_secret_key_reference",
    "{{secrets/<scope>/<key>}}",
    label="OpenAI Secret Key Reference",
)

dbutils.widgets.text(
    "endpoint_name",
    "my-hackathon-endpoint",
    label="Name of External Model Endpoint",
)

# COMMAND ----------

open_ai_secret_key_reference = dbutils.widgets.get("open_ai_secret_key_reference")
endpoint_name = dbutils.widgets.get("endpoint_name")

# COMMAND ----------

from mlflow.deployments import get_deploy_client

try:
    client = get_deploy_client("databricks")
    try:
        endpoint = client.get_endpoint(endpoint_name)
    except:
        endpoint = client.create_endpoint(
            name="chat",
            config={
                "served_entities": [
                    {
                        "name": "test",
                        "external_model": {
                            "name": "gpt-4-vision-preview",
                            "provider": "openai",
                            "task": "llm/v1/chat",
                            "openai_config": {
                                "openai_api_key": open_ai_secret_key_reference,
                            },
                        },
                    }
                ],
            },
        )
except:
    pass

# COMMAND ----------

