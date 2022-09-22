"""
Build Pyserini (PS) sparse index with data from DPR corpus.
This is needed as pyserini dpr index doesn't store original texts,
so one needs to also generate a sparse index from the identical corpus
to be able to recover back the original documents.
"""

import subprocess
import argparse
import shutil
import os
import json
import _jsonnet


WIKIPEDIA_CORPUSES_PATH = json.loads(_jsonnet.evaluate_file(".global_config.jsonnet"))["WIKIPEDIA_CORPUSES_PATH"]


def main():

    parser = argparse.ArgumentParser(description="Generate the sparse counterpart of the dpr corpus.")
    parser.add_argument(
        "dataset_name", help='name of the dataset', type=str,
        choices=(
            "hotpotqa", "strategyqa", "iirc", "2wikimultihopqa", "musique"
        )
    )
    parser.add_argument("--force", help='force delete before creating new index.',
                        action="store_true", default=False)
    args = parser.parse_args()

    assert 0 <= args.shard_index < args.num_shards

    corpus_path = os.path.join(WIKIPEDIA_CORPUSES_PATH, f"{args.dataset_name}-wikpedia-dpr-corpus")
    index_path = os.path.join(WIKIPEDIA_CORPUSES_PATH, f"{args.dataset_name}-wikpedia-dpr-sparse-index")

    if not os.path.exists(corpus_path):
        exit(f"The corpus_path (input) {corpus_path} not available.")

    if args.force:
        shutil.rmtree(index_path, ignore_errors=True)

    if os.path.exists(index_path) and os.listdir(index_path):
        exit(f"The non-empty index_path (output) {index_path} already exists.")

    command = f'''
python -m pyserini.index.lucene
    --collection JsonCollection
    --input {corpus_path}
    --index {index_path}
    --generator DefaultLuceneDocumentGenerator
    --threads 1 \
    --storePositions --storeDocvectors --storeRaw
'''.strip()
    print("Running command:")
    print(command)

    command = [e for e in command.split()]
    subprocess.run(command)


if __name__ == "__main__":
    main()
