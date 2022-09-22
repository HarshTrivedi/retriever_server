import json
from typing import List, Dict, Tuple

from blink_retriever import BLINK_MODELS_PATH, BlinkRetriever
from elasticsearch_retriever import ElasticsearchRetriever
from dpr_retriever import DprRetriever


class UnifiedRetriever:

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
        elasticsearch_host: str = "http://localhost/",
        elasticsearch_port: int = 9200,
        # Blink init args:
        blink_models_path: str = BLINK_MODELS_PATH,
        blink_faiss_index_type: str = "flat", # "flat" or "hnsw",
        blink_fast: bool = False,
        blink_top_k: int = 1,
        # DPR init args:
        dpr_faiss_index_type: str = "flat", # "flat" or "hnsw",
        dpr_query_model_path: str = "facebook/dpr-question_encoder-multiset-base",
        dpr_device: str = "cpu",
        # what to initialize:
        initialize_retrievers: Tuple[str, str] = ("blink", "elasticsearch", "dpr"),
    ):

        self._limit_to_abstracts = dataset_name == "hotpotqa"

        self._elasticsearch_retriever = None
        if "elasticsearch" in initialize_retrievers:
            self._elasticsearch_retriever = ElasticsearchRetriever(
                dataset_name=dataset_name,
                elasticsearch_host=elasticsearch_host,
                elasticsearch_port=elasticsearch_port,
            )

        self._blink_retriever = None
        if "blink" in initialize_retrievers:
            self._blink_retriever = BlinkRetriever(
                blink_models_path=blink_models_path,
                faiss_index=blink_faiss_index_type,
                fast=blink_fast,
                top_k=blink_top_k
            )

        self._dpr_retriever = None
        if "dpr" in initialize_retrievers:
            self._dpr_retriever = DprRetriever(
                dataset_name=dataset_name,
                index_type=dpr_faiss_index_type,
                device=dpr_device,
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
            "if allowed_titles are passed, document_type must be paragraph_text."

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

        results = []
        selected_titles = set()
        for blink_title in blink_titles:
            retrievals = self._elasticsearch_retriever.retrieve_titles(
                query_text=blink_title, max_hits_count=max_hits_count
            )

            for retrieval in retrievals:

                if retrieval["title"] not in selected_titles:
                    selected_titles.add(retrieval["title"])
                    results.append({
                        "title": retrieval["title"],
                        "paragraph_text": retrieval["paragraph_text"]
                    })

        return results


    def retrieve_from_dpr(
            self,
            query_text: str,
            max_hits_count: int = 3,
        ) -> List[Dict]:
        """
        Option 5: retrieve_from_dpr
        """
        if self._dpr_retriever is None:
            raise Exception("DPR retriever not initialized.")

        results = self._dpr_retriever.retrieve_paragraphs(query_text=query_text, max_hits_count=max_hits_count)
        return results
