import json
import _jsonnet
from typing import List, Dict
import argparse
import os

from pyserini.search import FaissSearcher, DprQueryEncoder
from pyserini.search.lucene import LuceneSearcher


WIKIPEDIA_CORPUSES_PATH = json.loads(_jsonnet.evaluate_file(".global_config.jsonnet"))["WIKIPEDIA_CORPUSES_PATH"]


class DprRetriever:
    def __init__(
            self,
            corpus_name: str,
            index_type: str,
            hf_query_model_name_or_path: str = "facebook/dpr-question_encoder-multiset-base",
            device: str = "cpu",
        ):
        assert index_type in ("flat", "hsnw")
        assert corpus_name != "auto", f"corpus_name auto is not valid for dpr_retriever."

        print("Loading DprQueryEncoder...")
        query_encoder = DprQueryEncoder(
            encoder_dir=hf_query_model_name_or_path,
            tokenizer_name=hf_query_model_name_or_path,
            device=device
        )

        print("Loading FaissSearcher...")

        if index_type == "flat":
            dpr_index_path = os.path.join(
                WIKIPEDIA_CORPUSES_PATH,
                f"{corpus_name}-wikpedia-dpr-{index_type}-index/part_full"
            )
        else: # hnsw
            dpr_index_path = os.path.join(
                WIKIPEDIA_CORPUSES_PATH,
                f"{corpus_name}-wikpedia-dpr-{index_type}-index"
            )
        self._dense_searcher = FaissSearcher(dpr_index_path, query_encoder)

        print("Loading SparseSearcher...")
        # Dense index doesn't store the original text documents, so we need to
        # generate+load the corresponding sparse index too.
        sparse_index_path = os.path.join(
            WIKIPEDIA_CORPUSES_PATH, f"{corpus_name}-wikpedia-dpr-sparse-index"
        )
        self._sparse_searcher = LuceneSearcher(sparse_index_path)


    def retrieve_paragraphs(
        self,
        query_text: str = None,
        max_hits_count: int = 10
    ) -> List[Dict]:

        hits = self._dense_searcher.search(query=query_text, k=max_hits_count)

        retrieval_results = []
        for hit in hits:
            doc = self._sparse_searcher.doc(hit.docid)
            document = json.loads(doc.raw())
            contents = document["contents"]
            title, paragraph_text, paragraph_index = contents.split("\n")
            paragraph_index = int(paragraph_index.strip())
            retrieval_result = {
                "title": title, "paragraph_text": paragraph_text,
                "paragraph_index": paragraph_index, "score": float(hit.score)
            }
            retrieval_results.append(retrieval_result)

        retrieval_results = sorted(retrieval_results, key=lambda e: e["score"], reverse=True)
        retrieval_results = retrieval_results[:max_hits_count]
        return retrieval_results


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="retrieve paragraphs via DPR.")
    parser.add_argument(
        "dataset_name", type=str, help="dataset_name",
        choices={"hotpotqa", "2wikimultihopqa", "musique_ans", "iirc", "strategyqa"}
    )
    args = parser.parse_args()

    print("Loading DPR retriever ...")
    retriever = DprRetriever(
        dataset_name=args.dataset_name,
        index_type="flat",
        hf_query_model_name_or_path="facebook/dpr-question_encoder-multiset-base",
        device="cpu",
    )

    print("\n\nRetrieving Paragraphs ...")
    results = retriever.retrieve_paragraphs("injuries")
    for result in results:
        print(result)
