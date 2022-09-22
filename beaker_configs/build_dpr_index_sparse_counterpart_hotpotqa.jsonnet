local dataset = "hotpotqa";
local global_config = import '../.global_config.jsonnet';
local wikipedia_corpuses_path = global_config["WIKIPEDIA_CORPUSES_PATH"];

local input_directory = wikipedia_corpuses_path + "/" + dataset + "-wikpedia-dpr-corpus";
local output_directory = wikipedia_corpuses_path + "/" + dataset + "-wikpedia-dpr-sparse-index";
local docker_filepath = "docker_files/Dockerfile_dpr";

local num_shards = 4;

{
    "command": "python build_dpr_index_sparse_counterpart.py " + dataset,
    "data_filepaths": [input_directory],
    "local_output_directory": output_directory,
    "beaker_output_directory": output_directory,
    "docker_filepath": docker_filepath,
    "envs": {
        "WIKIPEDIA_CORPUSES_PATH": wikipedia_corpuses_path,
        "TRANSFORMERS_CACHE": "~/.cache",
    },
    "gpu_count": 1,
    "cpu_count": 32,
    "memory": "100GiB",
    "parallel_run_count": num_shards,
    "cluster": "onperm-ai2",
}