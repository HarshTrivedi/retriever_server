# https://github.com/allenai/docker-images
# https://github.com/allenai/docker-images/pkgs/container/cuda/24038895?tag=11.2-ubuntu20.04-v0.0.15
FROM ghcr.io/allenai/cuda:11.2-ubuntu20.04-v0.0.15

COPY requirements.txt /run/requirements.txt
COPY index_corpus.py /run/index_corpus.py
COPY blink_elasticsearch_retriever.py /run/blink_elasticsearch_retriever.py
COPY blink_retriever.py /run/blink_retriever.py
COPY elasticsearch_retriever.py /run/elasticsearch_retriever.py
COPY retriever_server.py /run/retriever_server.py
COPY interactive_query.py /run/interactive_query.py

RUN git clone https://github.com/facebookresearch/BLINK /run/

RUN pip install -r BLINK/requirements.txt
RUN pip install -r requirements.txt

ENTRYPOINT ["bash", "-l"]

# Once you're in:
# cd /run
# Start ES /net/../aristo/harsht/bin/elasticsearch -d -pid pid
# python index_corpus.py hotpotqa or python interactive_query.py ... etc
