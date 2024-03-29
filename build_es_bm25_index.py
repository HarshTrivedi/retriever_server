"""
Build ES (Elasticsearch) BM25 Index.
"""

from typing import Dict, Set
import argparse, json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from typing import Any
import hashlib
import io
import csv
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


global_config = json.loads(_jsonnet.evaluate_file(".global_config.jsonnet"))
WIKIPEDIA_CORPUSES_PATH = global_config["WIKIPEDIA_CORPUSES_PATH"]
NATQ_PATH = global_config["NATQ_PATH"] # set default to '../natcq/processed_datasets/natq'?


def hash_object(o: Any) -> str:
    """Returns a character hash code of arbitrary Python objects."""
    m = hashlib.blake2b()
    with io.BytesIO() as buffer:
        dill.dump(o, buffer)
        m.update(buffer.getbuffer())
        return base58.b58encode(m.digest()).decode()


def combine_title_and_text(title: str, text: str) -> str:
    # don't strip as it may lose the structure
    # NOTE: This is used in natcq project also.
    # so make sure to change it there also if it's changed here.
    return " :::\n\n".join([title, text])


def get_cleaned_wikipedia_page_to_page_es_document(
    elasticsearch_index: str,
    wikipedia_page: Dict,
    indexed_page_ids: Set[str],
    metadata: Dict,
    show_repetition_warning: bool = False,
):
    page_title = wikipedia_page["title"]
    page_id = wikipedia_page["id"]
    page_url = wikipedia_page["url"]

    if page_id in indexed_page_ids:
        if show_repetition_warning:
            print(
                "WARNING: Looks like a repeated page_id is being indexed. Skipping it."
            )
        return None

    es_document = {
        "id": page_id,
        "title": page_title,
        "url": page_url,
        "data": json.dumps(wikipedia_page),
    }
    document = {
        "_op_type": "create",
        "_index": elasticsearch_index,
        "_id": metadata["idx"],
        "_source": es_document,
    }
    metadata["idx"] += 1

    indexed_page_ids.add(page_id)

    return document


def yield_cleaned_wikipedia_page_to_page_titles_documents(
    elasticsearch_index: str,
    wikipedia_page: Dict,
    indexed_page_ids: Set[str],
    metadata: Dict,
    show_repetition_warning: bool = False,
):
    page_title = wikipedia_page["title"]
    page_id = wikipedia_page["id"]
    page_url = wikipedia_page["url"]
    if page_id not in indexed_page_ids:
        es_document = {
            "id": page_id,
            "title": page_title,
            "paragraph_index": 0,
            "paragraph_text": "",
            "url": page_url,
            "is_abstract": True,
            "metadata": "",
        }
        document = {
            "_op_type": "create",
            "_index": elasticsearch_index,
            "_id": metadata["idx"],
            "_source": es_document,
        }
        yield (document)
        metadata["idx"] += 1
        indexed_page_ids.add(page_id)
    elif show_repetition_warning:
        print("WARNING: Looks like a repeated page_id is being indexed. Skipping it.")


def yield_cleaned_wikipedia_page_to_chunked_doc_es_documents(
    elasticsearch_index: str,
    wikipedia_page: Dict,
    indexed_sub_document_ids: Set[str],
    metadata: Dict,
    show_repetition_warning: bool = False,
):

    page_title = wikipedia_page["title"]
    page_id = wikipedia_page["id"]
    page_url = wikipedia_page["url"]

    is_abstract_added = False
    for section in wikipedia_page["sections"]:

        section_index = section["index"]
        section_path = section["path"]

        sub_document_infos = []
        plural = {
            "paragraph": "paragraphs",
            "list": "lists",
            "infobox": "infoboxes",
            "table": "tables",
        }
        for document_type in ["paragraph", "list", "infobox", "table"]:
            for document in section[plural[document_type]]:
                document_index = document["index"]
                document_id = document["id"]

                for sub_document in document[
                    f"chunked_{plural[document_type]}"
                ]:
                    sub_document_id = sub_document["id"]
                    sub_document_index = sub_document["index"]
                    sub_document_text = sub_document["text"]
                    sub_document_infos.append(
                        [
                            document_type,
                            document_id,
                            document_index,
                            sub_document_id,
                            sub_document_index,
                            sub_document_text,
                        ]
                    )

        for sub_document_info in sub_document_infos:

            (
                document_type,
                document_id,
                document_index,
                sub_document_id,
                sub_document_index,
                sub_document_text,
            ) = sub_document_info

            if sub_document_id in indexed_sub_document_ids:
                if show_repetition_warning:
                    print(
                        "WARNING: Looks like a repeated sub_document_id is being indexed. Skipping it."
                    )
                continue

            is_abstract = False
            if not is_abstract_added:
                is_abstract = True
                is_abstract_added = True

            # merging two fields because querying combined field works better+faster in ES.
            # Also use section_path, don't use title as it provides more context.
            # I am using it in the dpr as well.
            main_text = combine_title_and_text(section_path, sub_document_text)
            metadata_ = {
                "page_id": page_id,
                "document_type": document_type,
                "document_path": section_path,
            }
            es_document = {
                "id": sub_document_id,
                "title": page_title,
                "section_index": section_index,
                "section_path": section_path,
                "paragraph_type": document_type,
                "paragraph_index": document_index,
                "paragraph_sub_index": sub_document_index,
                "paragraph_text": main_text,
                "url": page_url,
                "is_abstract": is_abstract,
                "metadata": json.dumps(metadata_),
            }
            document = {
                "_op_type": "create",
                "_index": elasticsearch_index,
                "_id": metadata["idx"],
                "_source": es_document,
            }
            yield (document)
            metadata["idx"] += 1

            indexed_sub_document_ids.add(sub_document_id)


def make_hotpotqa_documents(elasticsearch_index: str, metadata: Dict = None):
    raw_glob_filepath = os.path.join(
        WIKIPEDIA_CORPUSES_PATH, "hotpotqa-wikpedia-paragraphs/*/wiki_*.bz2"
    )
    metadata = metadata or {"idx": 1}
    assert "idx" in metadata
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
                "_op_type": "create",
                "_index": elasticsearch_index,
                "_id": metadata["idx"],
                "_source": es_paragraph,
            }
            yield (document)
            metadata["idx"] += 1


def make_strategyqa_documents(elasticsearch_index: str, metadata: Dict = None):
    raw_glob_filepath = os.path.join(
        WIKIPEDIA_CORPUSES_PATH,
        "strategyqa-wikipedia-paragraphs/strategyqa-wikipedia-paragraphs.jsonl",
    )
    metadata = metadata or {"idx": 1}
    assert "idx" in metadata
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
                "_op_type": "create",
                "_index": elasticsearch_index,
                "_id": metadata["idx"],
                "_source": es_paragraph,
            }
            yield (document)
            metadata["idx"] += 1


def make_iirc_documents(elasticsearch_index: str, metadata: Dict = None):
    raw_filepath = os.path.join(
        WIKIPEDIA_CORPUSES_PATH, "iirc-wikipedia-paragraphs/context_articles.json"
    )
    metadata = metadata or {"idx": 1}
    assert "idx" in metadata

    random.seed(13370)  # Don't change.

    with open(raw_filepath, "r") as file:
        full_data = json.load(file)

        for title, page_html in tqdm(full_data.items()):
            page_soup = BeautifulSoup(page_html, "html.parser")
            paragraph_texts = [
                text
                for text in page_soup.text.split("\n")
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
                id_ = hash_object(title + paragraph_text)
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
                    "_op_type": "create",
                    "_index": elasticsearch_index,
                    "_id": metadata["idx"],
                    "_source": es_paragraph,
                }
                yield (document)
                metadata["idx"] += 1


def make_2wikimultihopqa_documents(elasticsearch_index: str, metadata: Dict = None):
    raw_filepaths = [
        os.path.join(
            WIKIPEDIA_CORPUSES_PATH, "2wikimultihopqa-wikipedia-paragraphs/train.json"
        ),
        os.path.join(
            WIKIPEDIA_CORPUSES_PATH, "2wikimultihopqa-wikipedia-paragraphs/dev.json"
        ),
        os.path.join(
            WIKIPEDIA_CORPUSES_PATH, "2wikimultihopqa-wikipedia-paragraphs/test.json"
        ),
    ]
    metadata = metadata or {"idx": 1}
    assert "idx" in metadata

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
                        "_op_type": "create",
                        "_index": elasticsearch_index,
                        "_id": metadata["idx"],
                        "_source": es_paragraph,
                    }
                    yield (document)
                    metadata["idx"] += 1


def make_musique_documents(elasticsearch_index: str, metadata: Dict = None):
    raw_filepaths = [
        os.path.join(
            WIKIPEDIA_CORPUSES_PATH,
            "musique-wikipedia-paragraphs/musique_ans_v1.0_dev.jsonl",
        ),
        os.path.join(
            WIKIPEDIA_CORPUSES_PATH,
            "musique-wikipedia-paragraphs/musique_ans_v1.0_test.jsonl",
        ),
        os.path.join(
            WIKIPEDIA_CORPUSES_PATH,
            "musique-wikipedia-paragraphs/musique_ans_v1.0_train.jsonl",
        ),
        os.path.join(
            WIKIPEDIA_CORPUSES_PATH,
            "musique-wikipedia-paragraphs/musique_full_v1.0_dev.jsonl",
        ),
        os.path.join(
            WIKIPEDIA_CORPUSES_PATH,
            "musique-wikipedia-paragraphs/musique_full_v1.0_test.jsonl",
        ),
        os.path.join(
            WIKIPEDIA_CORPUSES_PATH,
            "musique-wikipedia-paragraphs/musique_full_v1.0_train.jsonl",
        ),
    ]
    metadata = metadata or {"idx": 1}
    assert "idx" in metadata

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
                        "_op_type": "create",
                        "_index": elasticsearch_index,
                        "_id": metadata["idx"],
                        "_source": es_paragraph,
                    }
                    yield (document)
                    metadata["idx"] += 1


def make_hwm_documents(elasticsearch_index: str, metadata: Dict = None):
    assert metadata is None
    metadata = {"idx": 1}
    print("Indexing HotpotQA documents...")
    for document in make_hotpotqa_documents(elasticsearch_index, metadata):
        yield document
    print("Indexing 2WikiMultihopQA documents...")
    for document in make_2wikimultihopqa_documents(elasticsearch_index, metadata):
        yield document
    print("Indexing MuSiQue documents...")
    for document in make_musique_documents(elasticsearch_index, metadata):
        yield document


def make_official_dpr_docs_documents(elasticsearch_index: str, metadata: Dict = None):
    input_filepath = os.path.join(
        WIKIPEDIA_CORPUSES_PATH, "official-dpr-corpus", "psgs_w100.tsv.gz"
    )
    metadata = metadata or {"idx": 1}
    assert "idx" in metadata

    with gzip.open(input_filepath, "rt") as file:
        reader = csv.DictReader(file, delimiter="\t")
        for row in tqdm(reader):

            # merging two fields because querying combined field works better+faster in ES.
            main_text = combine_title_and_text(row["title"].strip(), row["text"].strip())
            es_paragraph = {
                "id": row["id"],
                "title": row["title"].strip(),
                "paragraph_index": 0,
                "paragraph_text": main_text,
                "url": "",
                "is_abstract": False,
            }
            document = {
                "_op_type": "create",
                "_index": elasticsearch_index,
                "_id": metadata["idx"],
                "_source": es_paragraph,
            }
            yield (document)
            metadata["idx"] += 1


def make_natcq_page_titles_documents(elasticsearch_index: str, metadata: Dict = None):

    raw_filepath = os.path.join(
        WIKIPEDIA_CORPUSES_PATH, "natcq-wikipedia-paragraphs/wikipedia_corpus.jsonl.gz"
    )
    metadata = metadata or {"idx": 1}
    assert "idx" in metadata

    random.seed(13370)  # Don't change.

    indexed_page_ids = set()

    with gzip.open(raw_filepath, mode="rt") as file:

        for line in tqdm(file):

            wikipedia_page = json.loads(line)
            for document in yield_cleaned_wikipedia_page_to_page_titles_documents(
                elasticsearch_index, wikipedia_page, indexed_page_ids, metadata,
                show_repetition_warning=True
            ):
                yield document


def make_natq_page_titles_documents(elasticsearch_index: str, metadata: Dict = None):

    input_filepaths = [
        os.path.join(NATQ_PATH, "dev.jsonl"),
        os.path.join(NATQ_PATH, "train.jsonl")
    ]

    metadata = metadata or {"idx": 1}
    assert "idx" in metadata

    random.seed(13370)  # Don't change.

    indexed_page_ids = set()

    for input_filepath in input_filepaths:

        with open(input_filepath, "r") as file:

            for line in tqdm(file):

                wikipedia_page = json.loads(line)["context_data"]
                for document in yield_cleaned_wikipedia_page_to_page_titles_documents(
                    elasticsearch_index, wikipedia_page, indexed_page_ids, metadata,
                    show_repetition_warning=False
                ):
                    yield document


def make_natcq_chunked_docs_documents(elasticsearch_index: str, metadata: Dict = None):

    raw_filepath = os.path.join(
        WIKIPEDIA_CORPUSES_PATH, "natcq-wikipedia-paragraphs/wikipedia_corpus.jsonl.gz"
    )
    metadata = metadata or {"idx": 1}
    assert "idx" in metadata

    random.seed(13370)  # Don't change.

    indexed_sub_document_ids = set()

    with gzip.open(raw_filepath, mode="rt") as file:

        for line in tqdm(file):

            wikipedia_page = json.loads(line)
            for document in yield_cleaned_wikipedia_page_to_chunked_doc_es_documents(
                elasticsearch_index, wikipedia_page, indexed_sub_document_ids, metadata,
                show_repetition_warning=True
            ):
                yield document


def make_natq_chunked_docs_documents(elasticsearch_index: str, metadata: Dict = None):

    input_filepaths = [
        os.path.join(NATQ_PATH, "dev.jsonl"),
        os.path.join(NATQ_PATH, "train.jsonl")
    ]

    metadata = metadata or {"idx": 1}
    assert "idx" in metadata

    random.seed(13370)  # Don't change.

    indexed_sub_document_ids = set()

    for input_filepath in input_filepaths:

        with open(input_filepath, "r") as file:

            for line in tqdm(file):

                wikipedia_page = json.loads(line)["context_data"]
                for document in yield_cleaned_wikipedia_page_to_chunked_doc_es_documents(
                    elasticsearch_index, wikipedia_page, indexed_sub_document_ids, metadata,
                    show_repetition_warning=False
                ):
                    yield document


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Index paragraphs in Elasticsearch")
    parser.add_argument(
        "dataset_name",
        help="name of the dataset",
        type=str,
        choices=(
            "hotpotqa",
            "strategyqa",
            "iirc",
            "2wikimultihopqa",
            "musique_ans",
            "hwm",
            "official_dpr_docs",
            "natcq_page_titles",
            "natq_page_titles",
            "natcq_chunked_docs",
            "natq_chunked_docs",
        ),
    )
    parser.add_argument(
        "--force",
        help="force delete before creating new index.",
        action="store_true",
        default=False,
    )
    args = parser.parse_args()

    # conntect elastic-search
    elastic_host = "localhost"
    elastic_port = 9200
    elasticsearch_index = f"{args.dataset_name}-wikipedia"
    es = Elasticsearch(
        [{"host": elastic_host, "port": elastic_port}],
        max_retries=2, # it's exp backoff starting 2, more than 2 retries will be too much.
        timeout=500,
        retry_on_timeout=True,
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
                "paragraph_index": {"type": "integer"},
                "paragraph_text": {
                    "type": "text",
                    "analyzer": "english",
                },
                "url": {
                    "type": "text",
                    "analyzer": "english",
                },
                "is_abstract": {"type": "boolean"},
            }
        }
    }

    if args.dataset_name in ("natcq_chunked_docs", "natq_chunked_docs"):
        paragraphs_index_settings["mappings"]["properties"] = {
            "metadata": {"type": "object", "index": False}
        }
        paragraphs_index_settings["mappings"]["properties"]["section_path"] = {
            "type": "text",
            "analyzer": "english",
        }
        paragraphs_index_settings["mappings"]["properties"]["paragraph_type"] = {
            "type": "text",
            "analyzer": "english",
        }

        paragraphs_index_settings["mappings"]["properties"]["paragraph_sub_index"] = {
            "type": "text",
            "analyzer": "english",
        }

    index_exists = es.indices.exists(elasticsearch_index)
    print("Index already exists" if index_exists else "Index doesn't exist.")

    # delete index if exists
    if index_exists:

        if not args.force:
            feedback = input(
                f"Index {elasticsearch_index} already exists. "
                f"Are you sure you want to delete it?"
            )
            if not (feedback.startswith("y") or feedback == ""):
                exit("Termited by user.")
        es.indices.delete(index=elasticsearch_index)

    # create index
    print("Creating Index ...")
    es.indices.create(
        index=elasticsearch_index, ignore=400, body=paragraphs_index_settings
    )

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
    elif args.dataset_name == "hwm":
        make_documents = make_hwm_documents
    elif args.dataset_name == "official_dpr_docs":
        make_documents = make_official_dpr_docs_documents
    elif args.dataset_name == "natcq_page_titles":
        make_documents = make_natcq_page_titles_documents
    elif args.dataset_name == "natq_page_titles":
        make_documents = make_natq_page_titles_documents
    elif args.dataset_name == "natcq_chunked_docs":
        make_documents = make_natcq_chunked_docs_documents
    elif args.dataset_name == "natq_chunked_docs":
        make_documents = make_natq_chunked_docs_documents
    else:
        raise Exception(f"Unknown dataset_name {args.dataset_name}")

    # Bulk-insert documents into index
    print("Inserting Paragraphs ...")
    result = bulk(
        es,
        make_documents(elasticsearch_index),
        raise_on_error=True, # set to true o/w it'll fail silently and only show less docs.
        raise_on_exception=True, # set to true o/w it'll fail silently and only show less docs.
        max_retries=2, # it's exp backoff starting 2, more than 2 retries will be too much.
        request_timeout=500,
        chunk_size=500,
    )
    es.indices.refresh(elasticsearch_index)  # actually updates the count.
    document_count = result[0]
    print(f"Index {elasticsearch_index} is ready. Added {document_count} documents.")
