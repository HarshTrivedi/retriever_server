from typing import List, Dict, Union
import argparse
import json

from tqdm import tqdm
from collections import OrderedDict
import elasticsearch
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


class ElasticsearchRetriever:

    def __init__(
            self,
            dataset_name: str,
            elastic_host: str = "http://localhost/",
            elastic_port: int = 9200,
        ):
        self._es = Elasticsearch([{'host': elastic_host, 'port': elastic_port}])
        self._index_name = f"{dataset_name}-wikipedia"

    def retrieve_paragraphs(
        self,
        query_text: str,
        is_abstract: bool = None,
        allowed_titles: List[str] = None,
        max_buffer_count: int = 100,
        max_hits_count: int = 10
    ) -> List[Dict]:

        query = {
            "size": max_buffer_count,
            "_source": ["id", "title", "text", "url", "is_abstract"], # what records are needed in result
            "query": {
                "bool": {
                    "must": [
                        {"match": {"paragraph": query_text}},
                    ],
                }
            }
        }

        if is_abstract is not None:
            query["query"]["bool"]["must"].append({"match": {"is_abstract": is_abstract}})

        result = self._es.search(index=self._index_name, body=query)

        retrieval = []
        if result.get('hits') is not None and result['hits'].get('hits') is not None:
            retrieval = result['hits']['hits']
            text2retrieval = OrderedDict()
            for item in retrieval:
                text = item["_source"]["text"].strip().lower()
                text2retrieval[text] = item
            retrieval = list(text2retrieval.values())[:max_hits_count]

        if allowed_titles is not None:
            retrieval = [item for item in retrieval if item["title"] in allowed_titles]

        return retrieval

    def retrieve_titles(
        self,
        query_text: str,
        max_buffer_count: int = 100,
        max_hits_count: int = 10
    ) -> List[Dict]:

        query = {
            "size": max_buffer_count,
            "_source": ["title", "url"], # what records are needed in result
            "query": {
                "bool": {
                    "must": [
                        {"match": {"paragraph": query_text}},
                        {"match": {"is_abstract": True}}, # so that same title doesn't show up many times.
                    ],
                }
            }
        }

        result = self._es.search(index=self._index_name, body=query)

        retrieval = []
        if result.get('hits') is not None and result['hits'].get('hits') is not None:
            retrieval = result['hits']['hits']
            text2retrieval = OrderedDict()
            for item in retrieval:
                text = item["_source"]["title"].strip().lower()
                text2retrieval[text] = item
            retrieval = list(text2retrieval.values())[:max_hits_count]

        return retrieval


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='retrieve paragraphs or titles')
    parser.add_argument(
        'dataset_name', type=str, help='dataset_name', choices={"hotpotqa", "strategyqa"}
    )
    parser.add_argument("--host", type=str, help="host", default="http://localhost/")
    parser.add_argument("--port", type=int, help="port", default=9200)
    args = parser.parse_args()

    retriever = ElasticsearchRetriever(
        args.host, args.port, args.dataset_name
    )

    results = retriever.retrieve_titles("injuries", "")

    for result in results:
        print(result)
