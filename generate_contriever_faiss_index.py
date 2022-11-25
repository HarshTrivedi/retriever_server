import argparse
from contriever_retriever import ContrieverRetriever


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="generate contriever embeddings")
    parser.add_argument(
        "dataset_name", type=str, help="dataset_name",
        choices={"original", "musique_ans", "hotpotqa", "2wikimultihopqa", "iirc"}
    )
    parser.add_argument(
        '--chunk_by_type', type=str, default=None, help="chunk_by_type", choices={None, "words", "sentences"}
    )
    args = parser.parse_args()

    if args.dataset_name != "original":
        assert args.chunk_by_type != None
        corpus_name = "musique" if args.dataset_name == "musique_ans" else args.dataset_name
        if args.chunk_by_type == "words":
            corpus_name = "word_chunked_" + corpus_name
        if args.chunk_by_type == "sentences":
            corpus_name = "sentence_chunked_" + corpus_name
    else:
        assert args.chunk_by_type == None
        corpus_name = "original"

    ContrieverRetriever(corpus_name=corpus_name)
