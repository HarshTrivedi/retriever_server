import requests
import json


def main() -> None:
    url = "http://bore.pub:26335/retrieve"
    params = {
        "retrieval_method": "retrieve_from_elasticsearch",
        "query_text": "barack obama",
        "max_hits_count": 10,
    }
    result = requests.post(url, json=params)
    print(json.dumps(result.json(), indent=4))


if __name__ == "__name__":
    main()
