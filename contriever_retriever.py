from typing import List, Dict, Union
from dataclasses import dataclass
import requests
import argparse
import glob
import os
import sys
import json
import time

import _jsonnet

CONTRIEVER_DATA_PATH = json.loads(_jsonnet.evaluate_file(".global_config.jsonnet"))["CONTRIEVER_DATA_PATH"]

sys.path.insert(0, "contriever") # Make sure to clone the repository and install contriever/requirements.txt

import src.contriever
import src.index
from passage_retrieval import embed_queries, index_encoded_data


@dataclass
class ContrieverConfig:
    paragraphs_path: str
    paragraphs_embeddings: str
    projection_size: int = 768
    n_subquantizers: int = 0
    n_bits: int = 8
    indexing_batch_size: int = 1000000
    model_name_or_path: str = "facebook/contriever"
    lowercase: bool = False
    normalize_text: bool = False
    per_gpu_batch_size: int = 0
    question_maxlength: int = 0


class ContrieverRetriever:

    def __init__(self, corpus_name: str):
        self._corpus_name = corpus_name

        contriever_data_path = os.path.join(CONTRIEVER_DATA_PATH, corpus_name)
        config = ContrieverConfig(
            os.path.join(contriever_data_path, "paragraphs.tsv"),
            os.path.join(contriever_data_path, "embeddings/*")
        )
        model, tokenizer, _ = src.contriever.load_retriever(config.model_name_or_path)
        model.eval()
        model = model.cuda()
        self.config = config

        if not os.path.exists(self.config.paragraphs_embeddings.replace("*", "")):
            raise Exception(f"Embeddings path ({self.config.paragraphs_embeddings}) not found.")

        if not os.path.exists(self.config.paragraphs_path):
            raise Exception(f"Data path ({self.config.paragraphs_path}) not found.")

        self.model = model
        self.tokenizer = tokenizer

        self.index = src.index.Indexer(
            self.config.projection_size, self.config.n_subquantizers, self.config.n_bits
        )

        # index all paragraphs
        print("(Maybe) indexing all paragraphs.")
        input_paths = glob.glob(self.config.paragraphs_embeddings)
        input_paths = sorted(input_paths)
        embeddings_dir = os.path.dirname(input_paths[0])
        index_path = os.path.join(embeddings_dir, "index.faiss")
        if os.path.exists(index_path):
            self.index.deserialize_from(embeddings_dir)
        else:
            print(f"Indexing paragraphs from files {input_paths}")
            start_time_indexing = time.time()
            index_encoded_data(self.index, input_paths, self.config.indexing_batch_size)
            print(f"Indexing time: {time.time()-start_time_indexing:.1f} s.")
            self.index.serialize(embeddings_dir)

        # load paragraphs
        print("Loading contriever paragraphs...")
        paragraphs = src.data.load_passages(self.config.paragraphs_path)
        self.paragraph_id_map = {x["id"]: x for x in paragraphs}
        del paragraphs
        print("...Done.")


    def retrieve_paragraphs(
        self,
        query_text: str,
        corpus_name: str,
        max_hits_count: int
    ) -> List[Dict]:

        query_embeddings = embed_queries(self.config, [query_text], self.model, self.tokenizer)
        paragraph_ids, _ = self.index.search_knn(query_embeddings, max_hits_count)[0]
        paragraphs = [self.paragraph_id_map[paragraph_id] for paragraph_id in paragraph_ids]
        assert corpus_name == self._corpus_name, \
            f"Mismatching corpus_names ({corpus_name} != {self._corpus_name})"

        retrieval = [
            {
                "id": paragraph_id,
                "title": paragraph["title"].strip(),
                "paragraph_text": paragraph["text"].strip(),
                "is_abstract": False,
                "url": None,
                "corpus_name": self._corpus_name,
            }
            for paragraph_id, paragraph in zip(paragraph_ids, paragraphs)
            if len(paragraph["text"].strip().split()) >= 5
        ]

        return retrieval


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='retrieve paragraphs')
    parser.add_argument(
        "dataset_name", type=str, help="dataset_name",
        choices={
            "original", "musique_ans", "hotpotqa", "2wikimultihopqa", "iirc",
            "chunked_musique_ans", "chunked_hotpotqa", "chunked_2wikimultihopqa", "chunked_iirc",
        }
    )
    args = parser.parse_args()

    retriever = ContrieverRetriever(corpus_name=args.dataset_name)
    print("\n\nRetrieving Paragraphs ...")
    results = retriever.retrieve_paragraphs("Who is the 44th president of USA.")
    for result in results:
        print(result)
