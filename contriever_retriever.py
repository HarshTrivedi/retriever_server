from typing import List, Dict
from dataclasses import dataclass
from collections import defaultdict
import numpy as np
import argparse
import glob
import os
import sys
import json
import time

from tqdm import tqdm
import _jsonnet

CONTRIEVER_DATA_PATH = json.loads(_jsonnet.evaluate_file(".global_config.jsonnet"))["CONTRIEVER_DATA_PATH"]

sys.path.insert(0, "contriever") # Make sure to clone the repository and install contriever/requirements.txt

import src.contriever
import src.index
from passage_retrieval import embed_queries, index_encoded_data


def normalize_title(title):
    return title.strip().lower().replace(" ", "")


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
        self.paragraph_id_map = {x["id"]: x for x in tqdm(paragraphs)}
        self.paragraph_title_to_index_ids = defaultdict(list)
        db_id_to_index_id = {db_id: index_id for index_id, db_id in tqdm(enumerate(self.index.index_id_to_db_id))}
        for paragraph in tqdm(paragraphs):
            title = normalize_title(paragraph["title"])
            index_id = db_id_to_index_id[paragraph["id"]]
            self.paragraph_title_to_index_ids[title].append(index_id)
        del db_id_to_index_id
        del paragraphs
        print("...Done.")


    def retrieve_paragraphs(
        self,
        query_text: str,
        corpus_name: str,
        max_hits_count: int,
        allowed_titles: List[str] = None,
    ) -> List[Dict]:

        query_embeddings = embed_queries(self.config, [query_text], self.model, self.tokenizer)

        if allowed_titles is None:
            paragraph_ids, scores = self.index.search_knn(query_embeddings, max_hits_count)[0]
        else:
            # NOTE: faiss > 1.7.3 is needed for this.
            allowed_titles = [normalize_title(title) for title in allowed_titles]
            allowed_index_ids = [
                id_ for title in allowed_titles for id_ in self.paragraph_title_to_index_ids[title]
            ]
            allowed_index_ids = np.array(allowed_index_ids, dtype=np.int64)
            paragraph_ids, scores = self.index.search_knn(
                query_embeddings, max_hits_count, allowed_index_ids=allowed_index_ids
            )[0]
            paragraph_ids_scores = [
                (paragraph_id, score) for paragraph_id, score in zip(paragraph_ids, scores)
                if normalize_title(self.paragraph_id_map[paragraph_id]["title"]) in allowed_titles
            ]
            paragraph_ids = [paragraph_id for paragraph_id, _ in paragraph_ids_scores]
            scores = [score for _, score in paragraph_ids_scores]

        paragraphs = [self.paragraph_id_map[paragraph_id] for paragraph_id in paragraph_ids]

        assert corpus_name == self._corpus_name, \
            f"Mismatching corpus_names ({corpus_name} != {self._corpus_name})"

        retrieval = [
            {
                "id": paragraph_id,
                "title": paragraph["title"].strip(),
                "paragraph_text": paragraph["text"].strip(),
                "score": float(score),
                "is_abstract": False,
                "url": None,
                "corpus_name": self._corpus_name,
            }
            for paragraph_id, paragraph, score in zip(paragraph_ids, paragraphs, scores)
            if len(paragraph["text"].strip().split()) >= 5
        ]

        return retrieval


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='retrieve paragraphs')
    parser.add_argument(
        "dataset_name", type=str, help="dataset_name",
        choices={"original", "musique_ans", "hotpotqa", "2wikimultihopqa", "iirc"}
    )
    parser.add_argument(
        '--chunk_by_type', type=str, default="none", help="chunk_by_type", choices={"none", "words", "sentences"}
    )
    args = parser.parse_args()

    if args.dataset_name != "original":
        corpus_name = "musique" if args.dataset_name == "musique_ans" else args.dataset_name
        if args.chunk_by_type == "words":
            corpus_name = "word_chunked_" + corpus_name
        if args.chunk_by_type == "sentences":
            corpus_name = "sentence_chunked_" + corpus_name
    else:
        assert args.chunk_by_type == "none"
        corpus_name = "original"

    retriever = ContrieverRetriever(corpus_name=corpus_name)
    print("\n\nRetrieving Paragraphs ...")
    results = retriever.retrieve_paragraphs(
        "Computer vision is an interdisciplinary field", corpus_name, 10, allowed_titles=["Computer vision"]
    )
    for result in results:
        print(json.dumps(result, indent=4))
