"""
Build ES (Elasticsearch) BM25 Index.
"""

from typing import List
import argparse, elasticsearch, json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from typing import Any
import hashlib
import io
import dill
import gzip
from tqdm import tqdm
import argparse
import glob
import bz2
import base58
import _jsonnet
from bs4 import BeautifulSoup
import os
import random


WIKIPEDIA_CORPUSES_PATH = json.loads(_jsonnet.evaluate_file(".global_config.jsonnet"))["WIKIPEDIA_CORPUSES_PATH"]


def hash_object(o: Any) -> str:
    """Returns a character hash code of arbitrary Python objects."""
    m = hashlib.blake2b()
    with io.BytesIO() as buffer:
        dill.dump(o, buffer)
        m.update(buffer.getbuffer())
        return base58.b58encode(m.digest()).decode()


def make_hotpotqa_documents(elasticsearch_index: str):
    raw_glob_filepath = os.path.join(
        WIKIPEDIA_CORPUSES_PATH, "hotpotqa-wikpedia-paragraphs/*/wiki_*.bz2"
    )
    _idx = 1
    for filepath in tqdm(glob.glob(raw_glob_filepath)):
        for datum in bz2.BZ2File(filepath).readlines():
            instance = json.loads(datum.strip())

            id_ = hash_object(instance)[:32]
            title = instance["title"]
            sentences_text = [e.strip() for e in instance["text"]]
            paragraph_text = " ".join(sentences_text)
            url = instance["url"]
            is_abstract = True
            paragraph_index = 0

            es_paragraph = {
                "id": id_,
                "title": title,
                "paragraph_index": paragraph_index,
                "paragraph_text": paragraph_text,
                "url": url,
                "is_abstract": is_abstract,
            }
            document = {
                "_op_type": 'create',
                '_index': elasticsearch_index,
                '_id': _idx,
                '_source': es_paragraph,
            }
            yield (document)
            _idx += 1


def make_strategyqa_documents(elasticsearch_index: str):
    raw_glob_filepath = os.path.join(
        WIKIPEDIA_CORPUSES_PATH, "strategyqa-wikipedia-paragraphs/strategyqa-wikipedia-paragraphs.jsonl"
    )
    _idx = 1
    with open(raw_glob_filepath, "r") as file:
        for line in tqdm(file):
            instance = json.loads(line.strip())

            id_ = hash_object(instance)[:32]
            title = instance["title"]
            paragraph_index = instance["para_id"] - 1
            assert paragraph_index >= 0
            paragraph_text = instance["para"]
            url = ""
            is_abstract = paragraph_index == 0

            es_paragraph = {
                "id": id_,
                "title": title,
                "paragraph_index": paragraph_index,
                "paragraph_text": paragraph_text,
                "url": url,
                "is_abstract": is_abstract,
            }
            document = {
                "_op_type": 'create',
                '_index': elasticsearch_index,
                '_id': _idx,
                '_source': es_paragraph,
            }
            yield (document)
            _idx += 1


def make_iirc_documents(elasticsearch_index: str):
    raw_filepath = os.path.join(
        WIKIPEDIA_CORPUSES_PATH, "iirc-wikipedia-paragraphs/context_articles.json"
    )
    _idx = 1

    random.seed(13370) # Don't change.

    with open(raw_filepath, "r") as file:
        full_data = json.load(file)

        for title, page_html in tqdm(full_data.items()):
            page_soup = BeautifulSoup(page_html, "html.parser")
            paragraph_texts = [
                text for text in page_soup.text.split("\n")
                if text.strip() and len(text.strip().split()) > 10
            ]

            # IIRC has a positional bias. 70% of the times, the first
            # is the supporting one, and almost all are in 1st 20.
            # So we scramble them to make it more challenging retrieval
            # problem.
            paragraph_indices_and_texts = [
                (paragraph_index, paragraph_text)
                for paragraph_index, paragraph_text in enumerate(paragraph_texts)
            ]
            random.shuffle(paragraph_indices_and_texts)
            for paragraph_index, paragraph_text in paragraph_indices_and_texts:
                url = ""
                id_ = hash_object(title+paragraph_text)
                is_abstract = paragraph_index == 0
                es_paragraph = {
                    "id": id_,
                    "title": title,
                    "paragraph_index": paragraph_index,
                    "paragraph_text": paragraph_text,
                    "url": url,
                    "is_abstract": is_abstract,
                }
                document = {
                    "_op_type": 'create',
                    '_index': elasticsearch_index,
                    '_id': _idx,
                    '_source': es_paragraph,
                }
                yield (document)
                _idx += 1


def make_2wikimultihopqa_documents(elasticsearch_index: str):
    raw_filepaths = [
        os.path.join(WIKIPEDIA_CORPUSES_PATH, "2wikimultihopqa-wikipedia-paragraphs/train.json"),
        os.path.join(WIKIPEDIA_CORPUSES_PATH, "2wikimultihopqa-wikipedia-paragraphs/dev.json"),
        os.path.join(WIKIPEDIA_CORPUSES_PATH, "2wikimultihopqa-wikipedia-paragraphs/test.json"),
    ]
    _idx = 1

    used_full_ids = set()
    for raw_filepath in raw_filepaths:

        with open(raw_filepath, "r") as file:
            full_data = json.load(file)
            for instance in tqdm(full_data):

                for paragraph in instance["context"]:

                    title = paragraph[0]
                    paragraph_text = " ".join(paragraph[1])
                    paragraph_index = 0
                    url = ""
                    is_abstract = paragraph_index == 0

                    full_id = hash_object(" ".join([title, paragraph_text]))
                    if full_id in used_full_ids:
                        continue

                    used_full_ids.add(full_id)
                    id_ = full_id[:32]

                    es_paragraph = {
                        "id": id_,
                        "title": title,
                        "paragraph_index": paragraph_index,
                        "paragraph_text": paragraph_text,
                        "url": url,
                        "is_abstract": is_abstract,
                    }
                    document = {
                        "_op_type": 'create',
                        '_index': elasticsearch_index,
                        '_id': _idx,
                        '_source': es_paragraph,
                    }
                    yield (document)
                    _idx += 1


def make_musique_documents(elasticsearch_index: str):
    raw_filepaths = [
        os.path.join(WIKIPEDIA_CORPUSES_PATH, "musique-wikipedia-paragraphs/musique_ans_v1.0_dev.jsonl"),
        os.path.join(WIKIPEDIA_CORPUSES_PATH, "musique-wikipedia-paragraphs/musique_ans_v1.0_test.jsonl"),
        os.path.join(WIKIPEDIA_CORPUSES_PATH, "musique-wikipedia-paragraphs/musique_ans_v1.0_train.jsonl"),
        os.path.join(WIKIPEDIA_CORPUSES_PATH, "musique-wikipedia-paragraphs/musique_full_v1.0_dev.jsonl"),
        os.path.join(WIKIPEDIA_CORPUSES_PATH, "musique-wikipedia-paragraphs/musique_full_v1.0_test.jsonl"),
        os.path.join(WIKIPEDIA_CORPUSES_PATH, "musique-wikipedia-paragraphs/musique_full_v1.0_train.jsonl"),
    ]
    _idx = 1

    used_full_ids = set()
    for raw_filepath in raw_filepaths:

        with open(raw_filepath, "r") as file:
            for line in tqdm(file.readlines()):
                if not line.strip():
                    continue
                instance = json.loads(line)

                for paragraph in instance["paragraphs"]:

                    title = paragraph["title"]
                    paragraph_text = paragraph["paragraph_text"]
                    paragraph_index = 0
                    url = ""
                    is_abstract = paragraph_index == 0

                    full_id = hash_object(" ".join([title, paragraph_text]))
                    if full_id in used_full_ids:
                        continue

                    used_full_ids.add(full_id)
                    id_ = full_id[:32]

                    es_paragraph = {
                        "id": id_,
                        "title": title,
                        "paragraph_index": paragraph_index,
                        "paragraph_text": paragraph_text,
                        "url": url,
                        "is_abstract": is_abstract,
                    }
                    document = {
                        "_op_type": 'create',
                        '_index': elasticsearch_index,
                        '_id': _idx,
                        '_source': es_paragraph,
                    }
                    yield (document)
                    _idx += 1


def make_natcq_documents(elasticsearch_index: str):
    raw_filepath = os.path.join(
        WIKIPEDIA_CORPUSES_PATH, "natcq-wikipedia-paragraphs/wikipedia_corpus.jsonl.gz"
    )
    _idx = 1

    random.seed(13370) # Don't change.

    with gzip.open(raw_filepath, mode="rt") as file:

        for line in tqdm(file):

            page_data = json.loads(line)
            page_title = page_data["title"]
            page_id = page_data["page_id"]
            page_url = page_data["url"]

            is_abstract_added = False
            for section_wise_data in page_data["sectionwise_data"]:

                section_index = section_wise_data["section_index"]
                section_breadcrumb = section_wise_data["section_breadcrumb"]

                for section_paragraph in section_wise_data["section_paragraphs"]:
                    document_index = section_paragraph["paragraph_index"]
                    document_text = section_paragraph["paragraph_object"]["text"]
                    document_parsed_data = section_paragraph["paragraph_object"]["parsed_data"]
                    document_type = "paragraph"

                for section_list in section_wise_data["section_lists"]:
                    document_index = section_list["list_index"]
                    document_text = section_list["list_object"]["text"]
                    document_parsed_data = section_list["list_object"]["parsed_data"]
                    document_type = "list"

                for section_infobox in section_wise_data["section_infoboxes"]:
                    document_index = section_infobox["infobox_index"]
                    document_text = section_infobox["infobox_object"]["text"]
                    document_parsed_data = section_infobox["infobox_object"]["parsed_data"]
                    document_type = "infobox"

                for section_table in section_wise_data["section_tables"]:
                    document_index = section_table["table_index"]
                    document_text = section_table["table_object"]["text"]
                    document_parsed_data = section_table["table_object"]["parsed_data"]
                    document_type = "table"

                document_id = hash_object([page_id, section_index, document_index])

                is_abstract = False
                if not is_abstract_added:
                    is_abstract = True
                    is_abstract_added = True

                metadata = {
                    "page_id": page_id,
                    "document_type": document_type,
                    "document_path": section_breadcrumb,
                    "document_parsed_data": document_parsed_data,
                }
                es_document = {
                    "id": document_id,
                    "title": page_title,
                    "section_index": section_index,
                    "paragraph_index": document_index,
                    "paragraph_text": document_text,
                    "url": page_url,
                    "is_abstract": is_abstract,
                    "metadata": json.dumps(metadata)
                }
                document = {
                    "_op_type": 'create',
                    '_index': elasticsearch_index,
                    '_id': _idx,
                    '_source': es_document,
                }
                yield (document)
                _idx += 1


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Index paragraphs in Elasticsearch')
    parser.add_argument(
        "dataset_name", help='name of the dataset', type=str,
        choices=(
            "hotpotqa", "strategyqa", "iirc", "2wikimultihopqa", "musique_ans", "natcq"
        )
    )
    parser.add_argument("--force", help='force delete before creating new index.',
                        action="store_true", default=False)
    args = parser.parse_args()

    # conntect elastic-search
    elastic_host = 'localhost'
    elastic_port = 9200
    elasticsearch_index = f"{args.dataset_name}-wikipedia"
    es = Elasticsearch(
        [{'host': elastic_host, 'port': elastic_port}],
        max_retries=10, timeout=30, retry_on_timeout=True
    )

    # INDEX settings:
    # Index name: {args.dataset_name}-wikipedia (database-name)
    # Type Name: paragraphs (table-name)
    # Properties (Field Names [type = datatype]) :
    # field1: title
    # field2: paragraph_index
    # field3: paragraph_text
    # field4: url
    # field4: is_abstract

    paragraphs_index_settings = {
        "mappings": {
                "properties": {
                    "title": {
                        "type": "text",
                        "analyzer": "english",
                    },
                    "paragraph_index": {
                        "type": "integer"
                    },
                    "paragraph_text": {
                        "type": "text",
                        "analyzer": "english",
                    },
                    "url": {
                        "type": "text",
                        "analyzer": "english",
                    },
                    "is_abstract": {
                        "type": "boolean"
                    }
                }
        }
    }

    if args.dataset_name == "natcq":
        paragraphs_index_settings["mappings"]["properties"] = {
            "metadata": {"type": "object", "index" : False}
        }

    index_exists = es.indices.exists(elasticsearch_index)
    print("Index already exists" if index_exists else "Index doesn't exist.")

    # delete index if exists
    if index_exists:

        if not args.force:
            feedback = input(f"Index {elasticsearch_index} already exists. "
                             f"Are you sure you want to delete it?")
            if not (feedback.startswith("y") or feedback == ""):
                exit("Termited by user.")
        es.indices.delete(index=elasticsearch_index)

    # create index
    print("Creating Index ...")
    es.indices.create(index=elasticsearch_index, ignore=400, body=paragraphs_index_settings)

    if args.dataset_name == "hotpotqa":
        make_documents = make_hotpotqa_documents
    elif args.dataset_name == "strategyqa":
        make_documents = make_strategyqa_documents
    elif args.dataset_name == "iirc":
        make_documents = make_iirc_documents
    elif args.dataset_name == "2wikimultihopqa":
        make_documents = make_2wikimultihopqa_documents
    elif args.dataset_name == "musique_ans":
        make_documents = make_musique_documents
    elif args.dataset_name == "natcq":
        make_documents = make_natcq_documents
    else:
        raise Exception(f"Unknown dataset_name {args.dataset_name}")

    # Bulk-insert documents into index
    print("Inserting Paragraphs ...")
    result = bulk(es, make_documents(elasticsearch_index))
    es.indices.refresh(elasticsearch_index) # actually updates the count.
    document_count = result[0]
    print(f"Index {elasticsearch_index} is ready. Added {document_count} documents.")
