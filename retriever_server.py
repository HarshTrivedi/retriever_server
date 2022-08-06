import _jsonnet
from fastapi import FastAPI, status, Response

from blink_elasticsearch_retriever import BlinkElasticsearchRetriever

retriever_init_args = json.loads(
    _jsonnet.evaluate_file(".retriever_config.jsonnet")
)
retriever = BlinkElasticsearchRetriever(**retriever_init_args)

app = FastAPI()

@app.get("/")
async def index():
    return {"message": f"Hello! This is a retriever server."}

@app.post("/retrieve_from_elasticsearch/")
async def retrieve_from_elasticsearch(
        arguments: Request # see the corresponding method in blink_elasticsearch_retriever.py
    ):
        retriever = get_retriever()
        arguments = await arguments.json()
        return retriever.retrieve_from_elasticsearch(**arguments)

@app.post("/retrieve_from_blink/")
async def retrieve_from_blink(
        arguments: Request # see the corresponding method in blink_elasticsearch_retriever.py
    ):
        retriever = get_retriever()
        arguments = await arguments.json()
        return retriever.retrieve_from_blink(**arguments)

@app.post("/retrieve_from_blink_and_elasticsearch/")
async def retrieve_from_blink_and_elasticsearch(
        arguments: Request # see the corresponding method in blink_elasticsearch_retriever.py
    ):
        retriever = get_retriever()
        arguments = await arguments.json()
        return retriever.retrieve_from_blink_and_elasticsearch(**arguments)
