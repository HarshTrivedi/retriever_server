import json
import _jsonnet
from time import perf_counter
from fastapi import FastAPI, status, Response, Request

from unified_retriever import UnifiedRetriever

retriever_init_args = json.loads(
    _jsonnet.evaluate_file(".retriever_config.jsonnet")
)
retriever = UnifiedRetriever(**retriever_init_args)

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
        start_time = perf_counter()
        retrieval = getattr(retriever, retrieval_method)(**arguments)

        for retrieval_ in retrieval:
            retrieval_["corpus_name"] = retriever_init_args["dataset_name"]

        end_time = perf_counter()
        time_in_seconds = round(end_time - start_time, 1)
        return {"retrieval": retrieval, "time_in_seconds": time_in_seconds}
