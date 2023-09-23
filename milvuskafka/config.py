import os
import yaml

class Configuration():
    def __init__(self, config_yaml_path = None):
        if config_yaml_path is not None:
            f = open(config_yaml_path)
            data_map = yaml.safe_load(f)
            f.close()
        else:
            data_map = {}
        self.KAKFA_POLL_TIMEOUT = data_map.get("KAKFA_POLL_TIMEOUT", os.environ.get("KAKFA_POLL_TIMEOUT", 1))
        self.KAFKA_REPLICATION_FACTOR = data_map.get("KAFKA_REPLICATION_FACTOR", os.environ.get("KAFKA_REPLICATION_FACTOR", 1))
        self.KAFKA_TOPICS = {
            "SEARCH_RESPONSE_TOPIC": "SearchResponse",
            "SEARCH_REQUEST_TOPIC": "SearchRequest",
            "INSERT_REQUEST_TOPIC": "InsertRequest",
            "SEARCH_EMBEDDING_TOPIC": "SearchEmbeddingRequest",
            "INSERT_EMBEDDING_TOPIC": "InsertEmbeddingRequest",

        }

        # Configs for KAFKA
        self.KAFKA_BASE_CONFIGS = {
            "bootstrap.servers": data_map.get("KAFKA_SERVER", os.environ.get("KAFKA_SERVER", "")),
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": data_map.get("KAFKA_USERNAME", os.environ.get("KAFKA_USERNAME", "")),
            "sasl.password": data_map.get("KAFKA_PASSWORD", os.environ.get("KAFKA_PASSWORD", "")),
            "linger.ms": data_map.get("KAFKA_LINGER_MS", os.environ.get("KAFKA_LINGER_MS", "10")),
            "enable.auto.commit": True
        }

        # Configs for Milvus
        self.MILVUS_COLLECTION = data_map.get("MILVUS_COLLECTION", os.environ.get('MILVUS_COLLECTION', "kafkalection"))
        self.MILVUS_URI = data_map.get("MILVUS_URI", os.environ.get('MILVUS_URI', "http://localhost:19530"))
        self.MILVUS_TOKEN = data_map.get("MILVUS_TOKEN", os.environ.get('MILVUS_TOKEN', ""))

        self.HACKER_NEWS_API_URL = "https://hacker-news.firebaseio.com/v0/newstories.json"
        self.HACKER_NEWS_PARSE_SLEEP = 60

        self.EMBEDDING_MODEL = data_map.get("EMBEDDING_MODEL", os.environ.get('EMBEDDING_MODEL', "BAAI/bge-small-en-v1.5"))
        self.EMBEDDING_DIM =  data_map.get("EMBEDDING_DIM", os.environ.get('EMBEDDING_DIM', 384))

        self.OPENAI_KEY =  data_map.get("OPENAI_KEY", os.environ.get('OPENAI_KEY', ""))

