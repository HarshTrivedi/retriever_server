from typing import List, Dict
import argparse
import os
import re
import _jsonnet
import json

from build_es_bm25_index import (
    make_hotpotqa_documents,
    make_strategyqa_documents,
    make_iirc_documents,
    make_2wikimultihopqa_documents,
    make_musique_documents,
)

CONTRIEVER_DATA_PATH = json.loads(_jsonnet.evaluate_file(".global_config.jsonnet"))["CONTRIEVER_DATA_PATH"]


def chunk_with_sliding_window(text: str, chunk_size: int, sliding_window_size: str) -> List[str]:
    tokens = text.split(" ")
    chunked_texts = []
    start_index = 0
    while True:
        end_index = start_index + chunk_size
        chunk_tokens = tokens[start_index:end_index]
        if len(chunk_tokens) <= 5:
            return chunked_texts
        chunked_texts.append(" ".join(chunk_tokens))
        start_index += sliding_window_size


def get_transformed_documents(
    es_document: Dict,
    do_chunks: bool = False,
) -> List[Dict]:
    source_document = es_document["_source"]
    title_text = source_document["title"]
    paragraph_text = source_document["paragraph_text"]
    if do_chunks:
        paragraph_texts = chunk_with_sliding_window(paragraph_text)
    else:
        paragraph_texts = [paragraph_text]
    return [{"title": title_text, "text": paragraph_text} for paragraph_text in paragraph_texts]


def main():

    parser = argparse.ArgumentParser(description="Create contriever injestible wiki format corpus.")
    parser.add_argument(
        "dataset_name", help='name of the dataset', type=str,
        choices=("hotpotqa", "strategyqa", "iirc", "2wikimultihopqa", "musique")
    )
    args = parser.parse_args()

    if args.dataset_name == "hotpotqa":
        make_documents = make_hotpotqa_documents
    elif args.dataset_name == "strategyqa":
        make_documents = make_strategyqa_documents
    elif args.dataset_name == "iirc":
        make_documents = make_iirc_documents
    elif args.dataset_name == "2wikimultihopqa":
        make_documents = make_2wikimultihopqa_documents
    elif args.dataset_name == "musique":
        make_documents = make_musique_documents
    else:
        raise Exception(f"Unknown dataset_name {args.dataset_name}")

    corpus_name = "musique" if args.dataset_name == "musique_ans" else args.dataset_name
    if args.do_chunks:
        corpus_name = "chunked_" + corpus_name
    contriever_data_directory = os.path.join(CONTRIEVER_DATA_PATH, corpus_name)
    contriever_paragraphs_file_path = os.path.join(contriever_data_directory, "paragraphs.tsv")

    with open(contriever_paragraphs_file_path, "w") as file:
        line = f"id\ttext\ttitle\n"
        file.write(line)
        index = 0
        for document in make_documents():
            documents = get_transformed_documents(document, do_chunks=args.do_chunks)
            for document in documents:
                title = re.sub(r' +', " ", document["title"].replace("\n", " ").replace("\t", " "))
                text = re.sub(r' +', " ", document["text"].replace("\n", " ").replace("\t", " "))
                index += 1
                line = f"{index}\t{text}\t{title}\n"
                file.write(line)


if __name__ == "__main__":
    main()
