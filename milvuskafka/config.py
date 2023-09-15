import os
import yaml

class Configuration():
    def init(self, config_yaml_path = None):
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
            "REQUEST_TOPIC": "EmbeddingRequest",
        }

        # Configs for KAFKA
        self.KAFKA_BASE_CONFIGS = {
            "bootstrap.servers": data_map.get("KAFKA_SERVER", os.environ.get("KAFKA_SERVER", "")),
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": data_map.get("KAFKA_USERNAME", os.environ.get("KAFKA_USERNAME", "")),
            "sasl.password": data_map.get("KAFKA_PASSWORD", os.environ.get("KAFKA_PASSWORD", "")),
            "queue.buffering.max.ms": data_map.get("KAFKA_QUEUE_MAX_MS", os.environ.get("KAFKA_QUEUE_MAX_MS", "10")),
        }

        # Configs for Milvus
        self.MILVUS_COLLECTION = data_map.get("MILVUS_COLLECTION", os.environ.get('MILVUS_COLLECTION', "kafkalection"))
        self.MILVUS_DIM = 384
        self.MILVUS_URI = data_map.get("MILVUS_URI", os.environ.get('MILVUS_URI', "http://localhost:19530"))
        self.MILVUS_TOKEN = data_map.get("MILVUS_TOKEN", os.environ.get('MILVUS_TOKEN', ""))

        self.HACKER_NEWS_API_URL = "https://hacker-news.firebaseio.com/v0/newstories.json"
        self.HACKER_NEWS_PARSE_SLEEP = 60
