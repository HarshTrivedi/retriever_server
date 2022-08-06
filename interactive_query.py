import requests
import json
import argparse

from pygments import highlight, lexers, formatters


def main():

    parser = argparse.ArgumentParser(description='Query retriever interactively.')
    parser.add_argument(
        "--retrieval_method", type=str, help="retrieval_method",
        choices={"retrieve_from_elasticsearch", "retrieve_from_blink", "retrieve_from_blink_and_elasticsearch"},
        require=True
    )
    parser.add_argument("--host", type=str, help="host", require=True)
    parser.add_argument("--port", type=int, help="port", require=True)
    parser.add_argument("--max_hits_count", type=int, help="max_hits_count", require=True)
    args = parser.parse_args()

    while True:
        query_text = input("Enter Query: ")

        params = {
            # choices: "retrieve_from_elasticsearch", "retrieve_from_blink", "retrieve_from_blink_and_elasticsearch"
            "retrieval_method": args.retrieval_method
            ####
            "query_text": query_text,
            "max_hits_count": 3,
        }
        response = requests.get(args.host.rstrip("/") + ":" + str(port) + "/retrieve", params=params)
        result = response.json()

        result_str = json.dumps(result, indent=4)

        result_str = highlight(result_str.encode("utf-8"), lexers.JsonLexer(), formatters.TerminalFormatter())
        print(result_str)


if __name__ == '__main__':
    main()
