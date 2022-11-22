from typing import List, Dict, Union
from dataclasses import dataclass
import requests
import argparse
import glob
import os
import sys
import json

import _jsonnet

CONTRIEVER_DATA_PATH = json.loads(_jsonnet.evaluate_file(".global_config.jsonnet"))["CONTRIEVER_DATA_PATH"]

sys.path.insert(0, "contriever") # Make sure to clone the repository and install contriever/requirements.txt

import src.contriever
import src.index


@dataclass
class ContrieverConfig:
    passages_path
    passages_embeddings
    projection_size = 768
    n_subquantizers = 0
    n_bits = 8
    indexing_batch_size = 1000000
    model_name_or_path = "facebook/contriever"
    lowercase: bool = False
    normalize_text: bool = False
    per_gpu_batch_size: int = 0
    question_maxlength: int = 0
    n_docs: int = 10


class ContrieverRetriever:

    def __init__(self, corpus_name: str):
        self._corpus_name = corpus_name

        contriever_data_path = os.path.join(CONTRIEVER_DATA_PATH, corpus_name)
        config = ContrieverConfig(
            os.path.join(contriever_data_path, "passages.tsv"),
            os.path.join(contriever_data_path, "embeddings")
        )
        model, tokenizer, _ = contriever_src.contriever.load_retriever(config.model_name_or_path)
        model.eval()
        model = model.cuda()

        self.index = contriever_src.index.Indexer(
            config.projection_size, config.n_subquantizers, config.n_bits
        )

        # index all passages
        print("(Maybe) indexing all passages.")
        input_paths = glob.glob(config.passages_embeddings)
        input_paths = sorted(input_paths)
        embeddings_dir = os.path.dirname(input_paths[0])
        index_path = os.path.join(embeddings_dir, "index.faiss")
        if os.path.exists(index_path):
            self.index.deserialize_from(embeddings_dir)
        else:
            print(f"Indexing passages from files {input_paths}")
            start_time_indexing = time.time()
            index_encoded_data(index, input_paths, config.indexing_batch_size)
            print(f"Indexing time: {time.time()-start_time_indexing:.1f} s.")
            self.index.serialize(embeddings_dir)

        # load passages
        print("Loading contriever passages...")
        passages = src.data.load_passages(config.passages_path)
        self.passage_id_map = {x["id"]: x for x in passages}
        del passages
        print("...Done.")


    def retrieve_paragraphs(
        self,
        query_text: str,
        corpus_name: str,
        max_hits_count: int
    ) -> List[Dict]:

        query_embeddings = embed_queries(self.config, [query_text], self.model, self.tokenizer)
        passage_ids = index.search_knn(query_embeddings, self.config.n_docs)[0]
        passages = [passage_id_map[passage_id] for passage_id in passage_ids]
        retrieval = [
            {
                "id": passage_ids[index],
                "title": passages[index]["title"],
                "paragraph_text": passages[index]["text"],
                "is_abstract": False,
                "url": None,
                "corpus_name": self.corpus_name,
            }
            for index, passage in enumerate(passages)
        ]

        return retrieval


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='retrieve paragraphs')
    parser.add_argument(
        "dataset_name", type=str, help="dataset_name",
        choices={"original", "hotpotqa", "strategyqa", "2wikimultihopqa", "iirc"}
    )
    args = parser.parse_args()

    retriever = ContrieverRetriever(corpus_name=corpus_name)

    print("\n\nRetrieving Paragraphs ...")
    results = retriever.retrieve_paragraphs("Who is the 44th president of USA.")
    for result in results:
        print(result)
