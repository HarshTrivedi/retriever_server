from typing import List, Dict, Any
from logging import Logger
from functools import lru_cache
import json
import copy

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.realpath(__file__)), "BLINK", "blink"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.realpath(__file__)), "BLINK"))

import blink.ner as NER
from torch.utils.data import DataLoader, SequentialSampler, TensorDataset
from blink.biencoder.biencoder import BiEncoderRanker, load_biencoder
from blink.crossencoder.crossencoder import CrossEncoderRanker, load_crossencoder
from blink.biencoder.data_process import (
    process_mention_data,
    get_candidate_representation,
)
import blink.candidate_ranking.utils as utils
from blink.crossencoder.train_cross import modify, evaluate
from blink.crossencoder.data_process import prepare_crossencoder_data
from blink.indexer.faiss_indexer import DenseFlatIndexer, DenseHNSWFlatIndexer

from main_dense import (
    modify,
    prepare_crossencoder_data,
    _annotate,
    _process_biencoder_dataloader,
    _run_biencoder,
    _process_crossencoder_dataloader,
    _run_crossencoder,
    _load_candidates,
)

BLINK_MODELS_PATH = "BLINK/models"

@lru_cache(maxsize=None)
def load_blink_and_ner_models(
        biencoder_config: str,
        biencoder_model: str,
        crossencoder_config: str,
        crossencoder_model: str,
        entity_catalogue: str,
        entity_encoding: str,
        faiss_index: str,
        index_path: str,
        fast: bool,
    ):
    print("Loading BLINK models...")
    blink_models = load_blink_models(
        biencoder_config=biencoder_config,
        biencoder_model=biencoder_model,
        crossencoder_config=crossencoder_config,
        crossencoder_model=crossencoder_model,
        entity_catalogue=entity_catalogue,
        entity_encoding=entity_encoding,
        faiss_index=faiss_index,
        index_path=index_path,
        fast=fast,
        logger=None
    )
    print("done.")

    print("Loading NER model...")
    ner_model = NER.get_model()
    print("done.")
    return blink_models, ner_model


def load_blink_models(
        biencoder_config: str,
        biencoder_model: str,
        crossencoder_config: str,
        crossencoder_model: str,
        entity_catalogue: str,
        entity_encoding: str,
        faiss_index: str,
        index_path: str,
        fast: bool = False,
        logger: Logger = None
    ):

    # load biencoder model
    if logger:
        logger.info("loading biencoder model")
    with open(biencoder_config) as json_file:
        biencoder_params = json.load(json_file)
        biencoder_params["path_to_model"] = biencoder_model
    biencoder = load_biencoder(biencoder_params)

    crossencoder = None
    crossencoder_params = None
    if not fast:
        # load crossencoder model
        if logger:
            logger.info("loading crossencoder model")
        with open(crossencoder_config) as json_file:
            crossencoder_params = json.load(json_file)
            crossencoder_params["path_to_model"] = crossencoder_model
        crossencoder = load_crossencoder(crossencoder_params)

    # load candidate entities
    if logger:
        logger.info("loading candidate entities")
    (
        candidate_encoding,
        title2id,
        id2title,
        id2text,
        wikipedia_id2local_id,
        faiss_indexer,
    ) = _load_candidates(
        entity_catalogue, 
        entity_encoding, 
        faiss_index=faiss_index, 
        index_path=index_path,
        logger=logger,
    )

    return {
        "biencoder": biencoder,
        "biencoder_params": biencoder_params,
        "crossencoder": crossencoder,
        "crossencoder_params": crossencoder_params,
        "candidate_encoding": candidate_encoding,
        "title2id": title2id,
        "id2title": id2title,
        "id2text": id2text,
        "wikipedia_id2local_id": wikipedia_id2local_id,
        "faiss_indexer": faiss_indexer,
    }


def _run_blink_prediction(
    query_text: str,
    top_k: int,
    fast: bool,
    ner_model: NER,
    biencoder: Any,
    biencoder_params: Any,
    crossencoder: Any,
    crossencoder_params: Any,
    candidate_encoding: Any,
    title2id: Any,
    id2title: Any,
    id2text: Any,
    wikipedia_id2local_id: Any,
    faiss_indexer=None,
    logger=None,
) -> List[Dict]:

    predictions = []

    id2url = {
        v: "https://en.wikipedia.org/wiki?curid=%s" % k
        for k, v in wikipedia_id2local_id.items()
    }

    # Identify mentions
    samples = _annotate(ner_model, [query_text])

    # don't look at labels
    keep_all = True

    # prepare the data for biencoder
    dataloader = _process_biencoder_dataloader(
        samples, biencoder.tokenizer, biencoder_params
    )

    # run biencoder
    labels, nns, scores = _run_biencoder(
        biencoder, dataloader, candidate_encoding, top_k, faiss_indexer
    )

    # print biencoder prediction
    for entity_list, sample in zip(nns, samples):
        e_id = entity_list[0]
        e_title = id2title[e_id]
        e_text = id2text[e_id]
        e_url = id2url[e_id]
        predictions.append({"id": e_id, "title": e_title, "text": e_text, "url": e_url})

    if fast:
        # use only biencoder
        return predictions
    else:
        predictions = []


    # prepare crossencoder data
    context_input, candidate_input, label_input = prepare_crossencoder_data(
        crossencoder.tokenizer, samples, labels, nns, id2title, id2text, keep_all,
    )

    context_input = modify(
        context_input, candidate_input, crossencoder_params["max_seq_length"]
    )

    dataloader = _process_crossencoder_dataloader(
        context_input, label_input, crossencoder_params
    )

    # run crossencoder and get accuracy
    accuracy, index_array, unsorted_scores = _run_crossencoder(
        crossencoder,
        dataloader,
        logger,
        context_len=biencoder_params["max_context_length"],
    )

    for entity_list, index_list, sample in zip(nns, index_array, samples):
        e_id = entity_list[index_list[-1]]
        e_title = id2title[e_id]
        e_text = id2text[e_id]
        e_url = id2url[e_id]
        predictions.append({"id": e_id, "title": e_title, "text": e_text, "url": e_url})

    return predictions


class BlinkRetriever:

    def __init__(
            self,
            blink_models_path: str = BLINK_MODELS_PATH,
            faiss_index: str = "flat", # "flat" or "hnsw",
            fast: bool = False,
            top_k: int = 1,
        ):

        assert faiss_index in ("flat", "hnsw")
        biencoder_model = os.path.join(blink_models_path, "biencoder_wiki_large.bin"),
        biencoder_config = os.path.join(blink_models_path, "biencoder_wiki_large.json"),
        entity_catalogue = os.path.join(blink_models_path, "entity.jsonl"),
        entity_encoding = os.path.join(blink_models_path, "all_entities_large.t7"),
        crossencoder_model = os.path.join(blink_models_path, "crossencoder_wiki_large.bin"),
        crossencoder_config = os.path.join(blink_models_path, "crossencoder_wiki_large.json"),

        if faiss_index == "flat":
            index_path = os.path.join(blink_models_path, "faiss_flat_index.pkl")
        else:
            index_path = os.path.join(blink_models_path, "faiss_hnsw_index.pkl")

        self._blink_models, self._ner_model = load_blink_and_ner_models(
            biencoder_model=biencoder_model,
            biencoder_config=biencoder_config,
            entity_catalogue=entity_catalogue,
            entity_encoding=entity_encoding,
            crossencoder_model=crossencoder_model,
            crossencoder_config=crossencoder_config,
            faiss_index=faiss_index,
            index_path=index_path,
            fast=fast,
        )
        self._top_k = top_k
        self._fast = fast

    def retrieve_paragraphs(self, query_text: str):
        arguments = {
            "query_text": query_text,
            "top_k": self._top_k,
            "fast": self._fast,
            "ner_model": self._ner_model,
            **self._blink_models
        }
        return _run_blink_prediction(**arguments)

def main():
    print("Call one ....")
    print(run_blink_prediction(query_text="BERT and ERNIE are Muppets."))

    print("Call two ....")
    print(run_blink_prediction(query_text="BERT and ERNIE are Muppets."))

    print("Call three ....")
    print(run_blink_prediction(query_text="BERT and ERNIE are Muppets."))


if __name__ == '__main__':
    main()
