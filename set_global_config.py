import socket
import json


def main():
    host_name = socket.gethostname()

    if "context" in host_name or "ambiguity" in host_name:
        print("Running from SBU machine.")
        config_dict = {
            "WIKIPEDIA_CORPUSES_PATH": "/shared_archive/hjtrivedi/wikipedia_corpuses",
            "TRANSFORMERS_CACHE": "~/.cache",
            "ELASTICSEARCH_PATH": "~/elasticsearch-7.10.2/bin/elasticsearch",
            "BLINK_MODELS_PATH": "BLINK/models",
            "NATQ_PATH": "../natcq/processed_datasets/natq/",
            "CONTRIEVER_DATA_PATH": "",
        }


    elif "cirrascale" in host_name:
        print("Running from Beaker machine.")
        config_dict = {
            "WIKIPEDIA_CORPUSES_PATH": "/net/nfs.cirrascale/aristo/harsht/wikipedia_corpuses",
            "TRANSFORMERS_CACHE": "/net/nfs.cirrascale/aristo/harsht/.hf_cache",
            "ELASTICSEARCH_PATH": "/net/nfs.cirrascale/aristo/harsht/elasticsearch-7.10.2/bin/elasticsearch",
            "BLINK_MODELS_PATH": "/net/nfs.cirrascale/aristo/harsht/blink_models",
            "NATQ_PATH": "/net/nfs.cirrascale/aristo/harsht/natcq_processed_datasets/natq",
            "CONTRIEVER_DATA_PATH": "",
        }


    elif "Harshs-MacBook-Pro" in host_name or "us-west-2.compute" in host_name:
        print("Running from Local machine.")
        config_dict = {
            "WIKIPEDIA_CORPUSES_PATH": "wikipedia_corpuses",
            "TRANSFORMERS_CACHE": "~/.cache",
            "ELASTICSEARCH_PATH": "elasticsearch",
            "BLINK_MODELS_PATH": "BLINK/models",
            "NATQ_PATH": "../natcq/processed_datasets/natq/",
            "CONTRIEVER_DATA_PATH": "",
        }

    else:
        raise ValueError(f"Unknown host name: {host_name}")

    config_file_path = ".global_config.jsonnet"
    print(f"Setting config in {config_file_path}")
    with open(config_file_path, "w") as file:
        message = "# Can be set automatically based on host by set_global_config.py\n"
        message += json.dumps(config_dict, indent=4)
        file.write(message)


if __name__ == "__main__":
    main()
