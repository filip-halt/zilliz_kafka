import os


KAKFA_POLL_TIMEOUT = 1
KAFKA_REPLICATION_FACTOR = os.environ.get('KAFKA_REPLICATION_FACTOR', 1)
KAFKA_TOPICS = {
    "SEARCH_RESPONSE_TOPIC": "SearchResponse",
    "SEARCH_REQUEST_TOPIC": "SearchRequest",
    "INSERT_REQUEST_TOPIC": "InsertRequest",
    "REQUEST_TOPIC": "EmbeddingRequest",
}

# Configs for KAFKA
KAFKA_DEFAULT_CONFIGS = {
    "bootstrap.servers": os.environ.get('KAFKA_SERVER'),
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ.get('KAFKA_USERNAME'),
    "sasl.password": os.environ.get('KAFKA_PASSWORD'),
    "queue.buffering.max.ms": os.environ.get('KAFKA_QUEUE_MAX_MS', "10"),

}

# Configs for Milvus
MILVUS_COLLECTION = os.environ.get('MILVUS_COLLECTION', "kafkalection")
MILVUS_DIM = 384
MILVUS_URI = os.environ.get('MILVUS_URI', "http://localhost:19530")
MILVUS_TOKEN = os.environ.get('MILVUS_TOKEN', "")

HACKER_NEWS_API_URL = "https://hacker-news.firebaseio.com/v0/newstories.json"
HACKER_NEWS_PARSE_SLEEP = 60
