import json
from typing import List, Dict, Tuple

from blink_retriever import BLINK_MODELS_PATH, BlinkRetriever
from elasticsearch_retriever import ElasticsearchRetriever


class BlinkElasticsearchRetriever:

    """
    Kinds of retrievals I need.

    Option 1: retrieve_from_elasticsearch
        Given some query text,
        1. Directly retrieve from elasticsearch

    Option 2: retrieve_from_blink
        Given some query text,
        1. Get blink titles
        2. Return abstract paragraphs corresponding to them (ignore the corpus).

    Option 3: retrieve_from_blink_and_elasticsearch (one_es_per_blink=False)
        Given some query text,
        1. Get blink titles
        2. For each blink title, find matching title from the given corpus.
        3. Make a single retrieval query and pick top K paras matching the query the most.

    Option 4: retrieve_from_blink_and_elasticsearch (one_es_per_blink=True)
        Given some query text,
        1. Get blink titles
        2. For each blink title, find matching title from the given corpus.
        3. For each of those titles, retrieve one paragraph from the given corpus.
    """


    def __init__(
        self,
        # Elasticsearch init args:
        dataset_name: str = "hotpotqa",
        elastic_host: str = "http://localhost/",
        elastic_port: int = 9200,
        # Blink init args:
        blink_models_path: str = BLINK_MODELS_PATH,
        faiss_index: str = "flat", # "flat" or "hnsw",
        fast: bool = False,
        top_k: int = 1,
        # what to initialize:
        initialize_retrievers: Tuple[str, str] = ("blink", "elasticsearch"),
    ):

        self._limit_to_abstracts = dataset_name == "hotpotqa"

        self._elasticsearch_retriever = None
        if "elasticsearch" in initialize_retrievers:
            self._elasticsearch_retriever = ElasticsearchRetriever(
                dataset_name=dataset_name,
                elastic_host=elastic_host,
                elastic_port=elastic_port,
            )

        self._blink_retriever = None
        if "blink" in initialize_retrievers:
            self._blink_retriever = BlinkRetriever(
                blink_models_path=blink_models_path,
                faiss_index=faiss_index,
                fast=fast,
                top_k=top_k
            )


    def retrieve_from_elasticsearch(
            self,
            query_text: str,
            max_hits_count: int = 3,
            document_type: str = "paragraph_text"
        ) -> List[Dict]:
        """
        Option 1: retrieve_from_elasticsearch
            Given some query text,
            1. Directly retrieve from elasticsearch (could be querying titles or paragraph_texts)
        """

        assert document_type in ("title", "paragraph_text")

        if self._elasticsearch_retriever is None:
            raise Exception("Elasticsearch retriever not initialized.")

        if document_type == "paragraph_text":
            paragraphs_results = self._elasticsearch_retriever.retrieve_paragraphs(
                query_text, is_abstract=self._limit_to_abstracts, max_hits_count=max_hits_count
            )
        elif document_type == "title":
            paragraphs_results = self._elasticsearch_retriever.retrieve_titles(
                query_text, max_hits_count=max_hits_count
            )

        return paragraphs_results


    def retrieve_from_blink(
            self,
            query_text: str,
            max_hits_count: int = 3,
        ) -> List[Dict]:
        """
        Option 2: retrieve_from_blink
            Given some query text,
            1. Get blink titles
            2. Return abstract paragraphs corresponding to them (ignore the corpus).
        """
        if self._blink_retriever is None:
            raise Exception("BLINK retriever not initialized.")

        blink_titles_results = self._blink_retriever.retrieve_paragraphs(query_text)

        results = [
            {"title": result["title"], "paragraph_text": result["text"]}
            for result in blink_titles_results
        ][:max_hits_count]
        return results


    def retrieve_from_blink_and_elasticsearch(
            self,
            query_text: str,
            one_es_per_blink: bool = True,
            max_hits_count: int = 3,
            skip_blink_titles: List = None,
        ) -> List[Dict]:
        """
        Option 3: retrieve_from_blink_and_elasticsearch (one_es_per_blink=False)
            Given some query text,
            1. Get blink titles
            2. For each blink title, find matching title from the given corpus.
            3. Make a single retrieval query and pick top K paras matching the query the most.

        Option 4: retrieve_from_blink_and_elasticsearch (one_es_per_blink=True)
            Given some query text,
            1. Get blink titles
            2. For each blink title, find matching title from the given corpus.
            3. For each of those titles, retrieve one paragraph from the given corpus.
        """

        if self._elasticsearch_retriever is None:
            raise Exception("Elasticsearch retriever not initialized.")

        if self._blink_retriever is None:
            raise Exception("BLINK retriever not initialized.")

        blink_titles_results = self._blink_retriever.retrieve_paragraphs(query_text)
        blink_titles = {result["title"] for result in blink_titles_results}

        skip_blink_titles = skip_blink_titles or []
        blink_titles = {
            blink_title for blink_title in blink_titles
            if blink_title not in skip_blink_titles
        }

        es_titles = [
            self._elasticsearch_retriever.retrieve_titles(query_text=blink_title, max_hits_count=1)[0]['title']
            for blink_title in blink_titles
        ]

        if not one_es_per_blink:
        # Option 3
            is_abstract = self._limit_to_abstracts
            results = self._elasticsearch_retriever.retrieve_paragraphs(
                query_text=query_text,
                is_abstract=is_abstract,
                allowed_titles=es_titles,
                max_hits_count=max_hits_count
            )

        else:
        # Option 4

            results = []
            for es_title in es_titles:
                is_abstract = self._limit_to_abstracts
                result = self._elasticsearch_retriever.retrieve_paragraphs(
                    query_text=query_text,
                    is_abstract=is_abstract,
                    allowed_titles=[es_title],
                    max_hits_count=1
                )
                assert len(result) <= 1
                results.extend(result)

        results = results[:max_hits_count]

        return results
