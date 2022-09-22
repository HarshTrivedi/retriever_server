import subprocess
import argparse
import shutil
import os
import json
import _jsonnet


WIKIPEDIA_CORPUSES_PATH = json.loads(_jsonnet.evaluate_file(".global_config.jsonnet"))["WIKIPEDIA_CORPUSES_PATH"]


def main():

    parser = argparse.ArgumentParser(description="Combine DPR sub-indices.")
    parser.add_argument(
        "dataset_name", help='name of the dataset', type=str,
        choices=(
            "hotpotqa", "strategyqa", "iirc", "2wikimultihopqa", "musique"
        )
    )
    parser.add_argument("--num-shards", help="number of total shards", type=int, default=1)
    args = parser.parse_args()

    assert 0 <= args.shard_index < args.num_shards

    flat_index_path = os.path.join(WIKIPEDIA_CORPUSES_PATH, f"{args.dataset_name}-wikpedia-dpr-flat-index", "part_")

    if not os.path.exists(flat_index_path):
        exit(f"The flat_index_path (input/output) {flat_index_path} not available.")

    command = f"python -m pyserini.index.merge_faiss_indexes --prefix {flat_index_path} --shard-num {args.num_shards}"
    print("Running command:")
    print(command)

    command = [e for e in command.split()]
    subprocess.run(command)


if __name__ == "__main__":
    main()
