import subprocess
import argparse
import shutil
import os
import json
import _jsonnet


WIKIPEDIA_CORPUSES_PATH = json.loads(_jsonnet.evaluate_file(".global_config.jsonnet"))["WIKIPEDIA_CORPUSES_PATH"]


def main():

    parser = argparse.ArgumentParser(description="Make faster versions of DPR indices.")
    parser.add_argument(
        "dataset_name", help='name of the dataset', type=str,
        choices=(
            "hotpotqa", "strategyqa", "iirc", "2wikimultihopqa", "musique"
        )
    )
    parser.add_argument("--num-shards", help="number of total shards", type=int, default=1)
    parser.add_argument("--force", help='force delete output directory, if it exits.',
                        action="store_true", default=False)
    args = parser.parse_args()

    assert 0 <= args.shard_index < args.num_shards

    flat_index_path = os.path.join(WIKIPEDIA_CORPUSES_PATH, f"{args.dataset_name}-wikpedia-dpr-flat-index")
    hnsw_index_path = os.path.join(WIKIPEDIA_CORPUSES_PATH, f"{args.dataset_name}-wikpedia-dpr-hnsw-index")

    if not os.path.exists(flat_index_path):
        exit(f"The flat_index_path (input) {flat_index_path} not available.")

    if args.force:
        shutil.rmtree(hnsw_index_path, ignore_errors=True)

    if os.path.exists(hnsw_index_path):
        exit(f"The hnsw_index_path (output) {hnsw_index_path} already exists.")

    command = f"python -m pyserini.index.faiss --input {flat_index_path} --output {hnsw_index_path} --hnsw"
    print("Running command:")
    print(command)

    command = [e for e in command.split()]
    subprocess.run(command) 


if __name__ == "__main__":
    main()
