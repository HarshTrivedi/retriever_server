# Writes a bashscript to set env in set_env.sh
# based on .global_config.jsonnet
# This is mainly required for running pyserini on aristo
# as the cache is in nfs.

import _jsonnet
import json

def main():
    config = json.loads(_jsonnet.evaluate_file(".global_config.jsonnet"))

    lines = ["#!/usr/bin/env bash"]
    for key, value in config.items():
        lines.append(f"export {key}={value}")

    output_filepath = "set_env.sh"
    with open(output_filepath, "w") as file:
        file.write("\n".join(lines).strip())

    print(f"source ./{output_filepath}")

if __name__ == '__main__':
    main()
