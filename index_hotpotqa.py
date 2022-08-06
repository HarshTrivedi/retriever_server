import argparse, elasticsearch, json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from tqdm import tqdm
import argparse
import glob
import bz2


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Index Elasticsearch')
    parser.add_argument("--force", help='force delete before creating new index.',
                        action="store_true", default=False)
    parser.add_argument("--skip-if-exists", help='skip indexing if it already exists.',
                        action="store_true", default=False)
    args = parser.parse_args()

    # conntect elastic-search
    elastic_host = 'localhost'
    elastic_port = 9200
    elasticsearch_index = "hotpotqa-wikipedia"
    es = Elasticsearch(
        [{'host': elastic_host, 'port': elastic_port}],
        max_retries=10, timeout=30, retry_on_timeout=True
    )

    # INDEX settings:
    # Index name: hotpotqa-wikipedia (database-name)
    # Type Name: paragraphs (table-name)
    # Properties (Field Names [type = datatype]) :
    # field1: idx [keyword]
    # field3: title
    # field4: text
    # field4: url
    # ...

    paragraphs_index_settings = {
        "mappings": {
                "properties": {
                    "idx": {
                        "type": "integer"
                    },
                    "title": {
                        "type": "text",
                        "analyzer": "english",
                    },
                    "text": {
                        "type": "text",
                        "analyzer": "english",
                    },
                    "url": {
                        "type": "text",
                        "analyzer": "english",
                    },
                }
        }
    }

    index_exists = es.indices.exists(elasticsearch_index)
    print("Index already exists" if index_exists else "Index doesn't exist.")

    # delete index if exists
    if index_exists:

        if args.skip_if_exists:
            print("Exiting as index exists and skip_if_exists is set to True.")
            exit()

        if not args.force:
            feedback = input(f"Index {elasticsearch_index} already exists. "
                             f"Are you sure you want to delete it?")
            if not (feedback.startswith("y") or feedback == ""):
                exit("Termited by user.")
        es.indices.delete(index=elasticsearch_index)

    # create index
    print("Creating Index ...")
    es.indices.create(index=elasticsearch_index, ignore=400, body=paragraphs_index_settings)

    # Add documents
    def make_documents():
        raw_glob_filepath = 'data/raw/hotpotqa-wikipedia/*/wiki_*.bz2'
        _idx = 1
        for filepath in tqdm(glob.glob(raw_glob_filepath)):
            for datum in bz2.BZ2File(filepath).readlines():
                instance = json.loads(datum.strip())
                id_ = int(instance["id"])
                title = instance["title"]
                sentences_text = [e.strip() for e in instance["text"]]
                paragraph_text = " ".join(sentences_text)
                url = instance["url"]
                es_fact = {
                    "id": id_,
                    "title": title,
                    "url": url,
                    "paragraph": paragraph_text,
                }
                document = {
                    "_op_type": 'create',
                    '_index': elasticsearch_index,
                    '_id': _idx,
                    '_source': es_fact
                }
                yield (document)
                _idx += 1

    # Bulk-insert documents into index
    print("Inserting Paragraphs ...")
    result = bulk(es, make_documents())
    es.indices.refresh(elasticsearch_index) # actually updates the count.
    document_count = result[0]
    print(f"Index {elasticsearch_index} is ready. Added {document_count} documents.")
