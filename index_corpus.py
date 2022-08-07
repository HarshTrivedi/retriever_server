import argparse, elasticsearch, json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from typing import Any
import hashlib
import io
import dill
from tqdm import tqdm
import argparse
import glob
import bz2
import base58


WIKIPEDIA_CORPUSES_PATH = json.loads(_jsonnet.evaluate_file(".global_config.jsonnet"))["WIKIPEDIA_CORPUSES_PATH"]


def hash_object(o: Any) -> str:
    """Returns a character hash code of arbitrary Python objects."""
    m = hashlib.blake2b()
    with io.BytesIO() as buffer:
        dill.dump(o, buffer)
        m.update(buffer.getbuffer())
        return base58.b58encode(m.digest()).decode()


def make_hotpotqa_documents():
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


def make_strategyqa_documents():
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


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Index paragraphs in Elasticsearch')
    parser.add_argument(
        "dataset_name", help='name of the dataset', type=str,
        choices=(
            "hotpotqa", "strategyqa",
            # "iirc"  "kilt", # TODO yet.
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
    else:
        raise Exception(f"Unknown dataset_name {dataset_name}")

    # Bulk-insert documents into index
    print("Inserting Paragraphs ...")
    result = bulk(es, make_documents())
    es.indices.refresh(elasticsearch_index) # actually updates the count.
    document_count = result[0]
    print(f"Index {elasticsearch_index} is ready. Added {document_count} documents.")
