import argparse
import os


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Combine DPR sub-indices.")
    parser.add_argument(
        "dataset_name", help='name of the dataset', type=str,
        choices=(
            "hotpotqa", "strategyqa", "iirc", "2wikimultihopqa", "musique"
        )
    )
    parser.add_argument("--num-shards", help="number of total shards", type=int, default=1)
    parser.add_argument("--force", help='force delete before creating new index.',
                        action="store_true", default=False)
    args = parser.parse_args()

    assert 0 <= args.shard_index < args.num_shards

    index_path = os.path.join(WIKIPEDIA_CORPUSES_PATH, f"{args.dataset_name}-wikpedia-dpr-index")

    if not os.path.exists(index_path):
        exit(f"The index_path {index_path} not available.")

    command = f"python -m pyserini.index.merge_faiss_indexes --prefix {index_path} --shard-num {args.num_shards}"

    command = [e for e in command.split()]
    subprocess.run(command)
