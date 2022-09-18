import argparse
import os


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Generate DPR index.")
    parser.add_argument(
        "dataset_name", help='name of the dataset', type=str,
        choices=(
            "hotpotqa", "strategyqa", "iirc", "2wikimultihopqa", "musique"
        )
    parser.add_argument("--shard-index", help="shard index", type=int, default=0))
    parser.add_argument("--num-shards", help="number of total shards", type=int, default=1)
    parser.add_argument("--batch-size", help="batch size", type=int, default=32)
    parser.add_argument("--force", help='force delete before creating new index.',
                        action="store_true", default=False)
    args = parser.parse_args()

    assert 0 <= args.shard_index < args.num_shards

    input_path = os.path.join(WIKIPEDIA_CORPUSES_PATH, f"{args.dataset_name}-wikpedia-dpr-input")
    output_path = os.path.join(WIKIPEDIA_CORPUSES_PATH, f"{args.dataset_name}-wikpedia-dpr-index")

    command = f'''
python -m pyserini.encode \
    input   --corpus {input_path} \
            --fields title paragraph_text paragraph_index \
            --delimiter "\\n" \
            --shard-id {args.shard_index} \
            --shard-num {args.num_shards} \
    output  --embeddings {output_path} \
            --to-faiss \
    encoder --encoder facebook/dpr-context_encoder-multiset-base \
            --fields paragraph_text \
            --max-length 300
            --device 0
            --batch {args.batch_size} \
            --fp16
'''.strip()

    index_path = os.path.join(WIKIPEDIA_CORPUSES_PATH, f"{args.dataset_name}-wikpedia-dpr-index")
    command = f"python -m pyserini.index.merge_faiss_indexes --prefix {index_path} --shard-num 4"

    command = [e for e in command.split()]
    subprocess.run(command)
