{
    ######## Elasticsearch init args: #########
    "dataset_name": "hotpotqa",
    "elastic_host": "http://localhost/",
    "elastic_port": 9200,

    ######## Blink init args: #################
    # blink_models_path is set in .global_config.jsonnet
    "blink_faiss_index": "flat", # "flat" or "hnsw",
    "blink_fast": false,
    "blink_top_k": 1,

    ######## DPR init args: ##################
    "dpr_faiss_index_type": "flat",
    "dpr_query_model_path": "facebook/dpr-question_encoder-multiset-base"

    ########## Retrievers to use: ############
    "initialize_retrievers": ["blink", "elasticsearch", "dpr"],
}
