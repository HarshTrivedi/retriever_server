{
    ######## Elasticsearch init args: #########
    "elasticsearch_dataset_name": "auto",
    "elasticsearch_host": "http://localhost/",
    "elasticsearch_port": 9200,

    ######## Blink init args: #################
    # blink_models_path is set in .global_config.jsonnet
    # "blink_faiss_index": "flat", # "flat" or "hnsw",
    # "blink_fast": false,
    # "blink_top_k": 1,

    ######## DPR init args: ##################
    # "dpr_faiss_index_type": "flat",
    # "dpr_query_model_path": "facebook/dpr-question_encoder-multiset-base"

    ######## Contriever init args: ##################
    # "contriever_dataset_name": "iirc",

    ########## Retrievers to use: ############
    "initialize_retrievers": ["elasticsearch"], # blink, elasticsearch, dpr, contriever
}
