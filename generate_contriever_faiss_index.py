import argparse
from contriever_retriever import ContrieverRetriever


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="generate contriever embeddings")
    parser.add_argument(
        "dataset_name", type=str, help="dataset_name",
        choices={
            "original", "musique_ans", "hotpotqa", "2wikimultihopqa", "iirc",
            "chunked_musique_ans", "chunked_hotpotqa", "chunked_2wikimultihopqa", "chunked_iirc",
        }
    )
    args = parser.parse_args()
    ContrieverRetriever(corpus_name=args.dataset_name)
