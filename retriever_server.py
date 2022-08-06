import json
import _jsonnet
from fastapi import FastAPI, status, Response, Request

from blink_elasticsearch_retriever import BlinkElasticsearchRetriever

retriever_init_args = json.loads(
    _jsonnet.evaluate_file(".retriever_config.jsonnet")
)
retriever = BlinkElasticsearchRetriever(**retriever_init_args)

app = FastAPI()

@app.get("/")
async def index():
    return {"message": f"Hello! This is a retriever server."}

@app.post("/retrieve/")
async def retrieve(
        arguments: Request # see the corresponding method in blink_elasticsearch_retriever.py
    ):
        arguments = await arguments.json()
        retrieval_method = arguments.pop("retrieval_method")
        assert retrieval_method in (
            "retrieve_from_elasticsearch",
            "retrieve_from_blink",
            "retrieve_from_blink_and_elasticsearch",
        )
        return getattr(retriever, retrieval_method)(**arguments)