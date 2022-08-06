from blink_retriever import BLINK_MODELS_PATH, ElasticsearchRetriever
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
        # Elasticsearch init args:
        dataset_name: str,
        elastic_host: str = "http://localhost/",
        elastic_port: int = 9200,
        # Blink init args:
        blink_models_path: str = BLINK_MODELS_PATH,
        faiss_index: str = "flat", # "flat" or "hnsw",
        fast: bool = False,
        top_k: int = 1,
    ):

        self._limit_to_abstracts = dataset_name == "hotpotqa"

        self._elasticsearch_retriever = ElasticsearchRetriever(
            dataset_name=dataset_name,
            elastic_host=elastic_host,
            elastic_port=elastic_port,
        )
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
            is_abstract: bool = None
        ) -> List[Dict]:
        """
        Option 1: retrieve_from_elasticsearch
            Given some query text,
            1. Directly retrieve from elasticsearch
        """
        paragraphs_results = self.elasticsearch_retriever.retrieve_paragraphs(
            query_text, is_abstract=is_abstract, max_hits_count=max_hits_count
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
        blink_titles_results = self.blink_retriever.retrieve_paragraphs(
            query_text, max_hits_count=max_hits_count
        )

        results = [
            {"title": result["title"], "text": result["text"]}
            for result in blink_titles_results
        ]
        raise results


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

        blink_titles_results = self.blink_retriever.retrieve_paragraphs(
            query_text, max_hits_count=max_hits_count
        )
        blink_titles = {result["title"] for result in blink_titles_results}

        skip_blink_titles = skip_blink_titles or []
        blink_titles = {
            blink_title for blink_title in blink_titles
            if blink_title not in skip_blink_titles
        }

        es_titles = [
            self.retrieve_titles(query_text=blink_title, max_hits_count=1)[0]
            for blink_title in blink_titles
        ]

        if not one_es_per_blink:
        # Option 3
            is_abstract = self._limit_to_abstracts
            results = self.retrieve_paragraphs(
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
                result = self.retrieve_paragraphs(
                    query_text=query_text,
                    is_abstract=is_abstract,
                    allowed_titles=[es_title],
                    max_hits_count=1
                )
                assert len(result) == 1
                results.append(result[0])

        results = sorted(results, key=lambda e: e["score"], reverse=True)[:max_hits_count]

        return results
