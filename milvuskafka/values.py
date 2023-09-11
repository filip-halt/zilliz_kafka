KAKFA_POLL_TIMEOUT = 1
KAFKA_REPLICATION_FACTOR = 1 # 3 for cloud
KAFKA_TOPICS = {
    "SEARCH_RESPONSE_TOPIC": "SearchResponse",
    "SEARCH_REQUEST_TOPIC": "SearchRequest",
    "INSERT_REQUEST_TOPIC": "InsertRequest",
    "REQUST_TOPIC": "EmbeddingRequest",
}
KAFKA_DEFAULT_CONFIGS = {
    "bootstrap.servers": "localhost:9094",
}
# KAFKA_DEFAULT_CONFIGS = {
#     "bootstrap.servers": "redacted",
#     "security.protocol": "SASL_SSL",
#     "sasl.mechanisms": "PLAIN",
#     "sasl.username": "redacted",
#     "sasl.password": "redacted"
# }

MILVUS_COLLECTION = "kafkalection"
MILVUS_DIM = 3

MILVUS_URI = "http://localhost:19530"
MILVUS_TOKEN = ""

HACKER_NEWS_API_URL = 'https://hacker-news.firebaseio.com/v0/newstories.json'
HACKER_NEWS_PARSE_SLEEP = 60
