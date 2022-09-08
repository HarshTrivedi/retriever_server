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
            document_type: str = "paragraph_text",
            allowed_titles: List[str] = None,
            paragraph_index: int = None,
        ) -> List[Dict]:
        """
        Option 1: retrieve_from_elasticsearch
            Given some query text,
            1. Directly retrieve from elasticsearch (could be querying titles or paragraph_texts)
        """

        assert document_type in ("title", "paragraph_text")

        if allowed_titles is not None:
            assert document_type == "paragraph_text", \
            "allowed_titles not valid input for the document_type of paragraph_text."

        if paragraph_index is not None:
            assert document_type == "paragraph_text", \
            "paragraph_index not valid input for the document_type of paragraph_text."

        if self._elasticsearch_retriever is None:
            raise Exception("Elasticsearch retriever not initialized.")

        if document_type == "paragraph_text":
            is_abstract = True if self._limit_to_abstracts else None # Note "None" and not False
            paragraphs_results = self._elasticsearch_retriever.retrieve_paragraphs(
                query_text, is_abstract=is_abstract, max_hits_count=max_hits_count,
                allowed_titles=allowed_titles, paragraph_index=paragraph_index
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
        EDIT: Removed option 3 and 4. Instead it just maps blink titles to corpus titles by retrieval.
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

        blink_titles_paras = [
            self._elasticsearch_retriever.retrieve_titles(query_text=blink_title, max_hits_count=1)[0]
            for blink_title in blink_titles
        ]

        results = [{"title": e["title"], "paragraph_text": e["paragraph_text"]} for e in blink_titles_paras]
        results = results[:max_hits_count]

        return results
