local dataset = "hotpotqa";
local global_config = import '../.global_config.jsonnet';
local wikipedia_corpuses_path = global_config["WIKIPEDIA_CORPUSES_PATH"];

local input_directories = [
    wikipedia_corpuses_path + "/" + dataset + "-wikpedia-dpr-flat-index/part_0",
    wikipedia_corpuses_path + "/" + dataset + "-wikpedia-dpr-flat-index/part_1",
    wikipedia_corpuses_path + "/" + dataset + "-wikpedia-dpr-flat-index/part_2",
    wikipedia_corpuses_path + "/" + dataset + "-wikpedia-dpr-flat-index/part_3",
];
local output_directory = wikipedia_corpuses_path + "/" + dataset + "-wikpedia-dpr-flat-index/full";
local docker_filepath = "docker_files/Dockerfile_dpr";
local num_shards = std.length(input_directories);

{
    "command": "python build_dpr_index_merge_parts.py " + dataset + " --num-shards " + num_shards,
    "data_filepaths": input_directories,
    "local_output_directory": output_directory,
    "beaker_output_directory": output_directory,
    "docker_filepath": docker_filepath,
    "envs": {
        "WIKIPEDIA_CORPUSES_PATH": wikipedia_corpuses_path,
        "TRANSFORMERS_CACHE": "~/.cache",
    },
    "gpu_count": 0,
    "cpu_count": 32,
    "memory": "100GiB",
    "parallel_run_count": 1,
    "cluster": "onperm-ai2",
}