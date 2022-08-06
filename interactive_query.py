import requests
import json
import argparse

from pygments import highlight, lexers, formatters


def main():

    parser = argparse.ArgumentParser(description='Query retriever interactively.')
    parser.add_argument(
        "--retrieval_method", type=str, help="retrieval_method",
        choices={"retrieve_from_elasticsearch", "retrieve_from_blink", "retrieve_from_blink_and_elasticsearch"},
        required=True
    )
    parser.add_argument("--host", type=str, help="host", required=False)
    parser.add_argument("--port", type=int, help="port", required=False)
    parser.add_argument("--max_hits_count", type=int, help="max_hits_count", default=3, required=False)
    args = parser.parse_args()

    if args.retrieval_method != "retrieve_from_blink" and (not args.host or not args.port):
        exit("For non blink-only retrievers, you need to pass the host and the port.")

    while True:
        query_text = input("Enter Query: ")

        params = {
            # choices: "retrieve_from_elasticsearch", "retrieve_from_blink", "retrieve_from_blink_and_elasticsearch"
            "retrieval_method": args.retrieval_method,
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
