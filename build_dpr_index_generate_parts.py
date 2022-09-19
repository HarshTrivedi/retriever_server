import subprocess
import argparse
import shutil
import os
import json
import _jsonnet


WIKIPEDIA_CORPUSES_PATH = json.loads(_jsonnet.evaluate_file(".global_config.jsonnet"))["WIKIPEDIA_CORPUSES_PATH"]


def main():

    parser = argparse.ArgumentParser(description="Generate DPR index.")
    parser.add_argument(
        "dataset_name", help='name of the dataset', type=str,
        choices=(
            "hotpotqa", "strategyqa", "iirc", "2wikimultihopqa", "musique"
        )
    )
    parser.add_argument("--shard-index", help="shard index", type=int, default=0)
    parser.add_argument("--num-shards", help="number of total shards", type=int, default=1)
    parser.add_argument("--batch-size", help="batch size", type=int, default=32)
    parser.add_argument("--force", help='force delete before creating new index.',
                        action="store_true", default=False)
    args = parser.parse_args()

    assert 0 <= args.shard_index < args.num_shards

    corpus_path = os.path.join(WIKIPEDIA_CORPUSES_PATH, f"{args.dataset_name}-wikpedia-dpr-corpus")
    flat_index_path = os.path.join(WIKIPEDIA_CORPUSES_PATH, f"{args.dataset_name}-wikpedia-dpr-flat-index")

    if not os.path.exists(corpus_path):
        exit(f"The corpus_path (input) {corpus_path} not available.")

    if args.force:
        shutil.rmtree(flat_index_path, ignore_errors=True)

    if os.path.exists(flat_index_path) and os.listdir(flat_index_path):
        exit(f"The non-empty flat_index_path (output) {flat_index_path} already exists.")


    command = f'''
python -m pyserini.encode \
    input   --corpus {corpus_path} \
            --fields title paragraph_text paragraph_index \
            --delimiter "\\n" \
            --shard-id {args.shard_index} \
            --shard-num {args.num_shards} \
    output  --embeddings {flat_index_path} \
            --to-faiss \
    encoder --encoder facebook/dpr-ctx_encoder-multiset-base \
            --fields paragraph_text \
            --max-length 300
            --device cuda:0
            --batch {args.batch_size} \
            --fp16
'''.strip()
    print("Running command:")
    print(command)

    command = [e for e in command.split()]
    subprocess.run(command)


if __name__ == "__main__":
    main()
