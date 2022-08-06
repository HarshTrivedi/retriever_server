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
brew install elastic/tap/elasticsearch-full

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
wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-7.10.2-linux-x86_64.tar.gz
wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-7.10.2-linux-x86_64.tar.gz.sha512
shasum -a 512 -c elasticsearch-7.10.2-linux-x86_64.tar.gz.sha512
tar -xzf elasticsearch-7.10.2-linux-x86_64.tar.gz
cd elasticsearch-7.10.2/
```

### Watermark Exceeded Error:

```bash
./fix_es_watermark.sh
```

### Check ES Indices

```bash
curl http://localhost:9200/_cat/indices\?pretty
```

## Start Main Retriever Server

```bash
uvicorn retriever_server:app --reload --port 8000
```


## Index Corpuses

```bash
python index_corpus.py hotpotqa
python index_corpus.py strategyqa
```
