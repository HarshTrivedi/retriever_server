# Installations


## BLINK

```bash

git clone https://github.com/facebookresearch/BLINK

# Either do this ...
beaker dataset fetch harsh-trivedi/blink_models --output BLINK/models
# ... or 
cd BLINK
chmod +x download_blink_models.sh
./download_blink_models.sh
wget http://dl.fbaipublicfiles.com/BLINK//faiss_flat_index.pkl -o models/
wget http://dl.fbaipublicfiles.com/BLINK/faiss_hnsw_index.pkl -o models/
cd ..

conda create -n retriever_server python=3.8 -y && conda activate retriever_server

pip install -r BLINK/requirements.txt
pip install -r requirements.txt
```

## Elasticsearch

Version requirement: 7.10

### Mac

```bash
# Option 1:
# Source: https://www.elastic.co/guide/en/elasticsearch/reference/current/brew.html
brew tap elastic/tap
brew install elastic/tap/elasticsearch-full # if it doesn't work: try 'brew untap elastic/tap' first: untap>tap>install.
# Then,
brew services start elastic/tap/elasticsearch-full
brew services stop elastic/tap/elasticsearch-full

# Option 2:
# Source: https://www.elastic.co/guide/en/elasticsearch/reference/current/targz.html
wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-7.10.2-darwin-x86_64.tar.gz
wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-7.10.2-darwin-x86_64.tar.gz.sha512
shasum -a 512 -c elasticsearch-7.10.2-darwin-x86_64.tar.gz.sha512
tar -xzf elasticsearch-7.10.2-darwin-x86_64.tar.gz
cd elasticsearch-7.10.2/
```

### Linux

```bash
# https://www.elastic.co/guide/en/elasticsearch/reference/8.1/targz.html
wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-7.10.2-linux-x86_64.tar.gz
wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-7.10.2-linux-x86_64.tar.gz.sha512
shasum -a 512 -c elasticsearch-7.10.2-linux-x86_64.tar.gz.sha512
tar -xzf elasticsearch-7.10.2-linux-x86_64.tar.gz
cd elasticsearch-7.10.2/
```

### Start and Stop

```bash
# start
./bin/elasticsearch --daemonize
# or ./bin/elasticsearch -d -p pid

# stop
pkill -f elasticsearch
# or pkill -F pid
```


### Watermark Exceeded Error:

```bash
./fix_es_watermark.sh
```

### Check ES Indices

```bash
curl http://localhost:9200/_cat/indices\?pretty
```

## Index Corpuses

```bash
python index_corpus.py hotpotqa
python index_corpus.py strategyqa
```

## Start Elasticsearch Server

```bash
./bin/elasticsearch --port 9200 # make sure to use this port
```

## Where's ES data stored and how to change it?

```bash
# See where it is stored.
curl http://127.0.0.1:9200/_nodes/stats/fs\?pretty
# You can change it in the elasticsearch.yml
# For mac, see https://www.elastic.co/guide/en/elasticsearch/reference/7.17/brew.html#brew-layout
```


## Start Main Retriever Server

```bash
# Tab 1
uvicorn retriever_server:app --reload --port 8000

# Tab 2
ngrok http 8000
```

## Interactive Querying

```bash
python interactive_query.py --retrieval_method retrieve_from_blink --host TODO --port 8000
```
