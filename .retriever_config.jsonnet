{
    # Elasticsearch init args:
    "dataset_name": "hotpotqa",
    "elastic_host": "http://localhost/",
    "elastic_port": 9200,
    # Blink init args:
    # blink_models_path is set in .global_config.jsonnet
    "faiss_index": "flat", # "flat" or "hnsw",
    "fast": false,
    "top_k": 1,
    # Retrievers:
    "initialize_retrievers": ["elasticsearch"], # ["blink", "elasticsearch"]
}
