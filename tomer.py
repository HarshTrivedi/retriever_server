import requests
import json


def main() -> None:
    url = "http://bore.pub:26335/retrieve"
    params = {
        "retrieval_method": "retrieve_from_elasticsearch",
        "query_text": "barack obama",
        "max_hits_count": 10,
        "corpus_name": "natcq_chunked_docs",
    }
    result = requests.post(url, json=params)
    if result.status_code == 200:
        print(json.dumps(result.json(), indent=4))
    else:
        print("Something went wrong!")


if __name__ == "__main__":
    main()
