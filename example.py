from pprint import pprint
import time
from milvuskafka.runner import Runner

r = Runner("config.yaml")
# Create the Milvus Collection and Kafka Topics
r.setup()
# Start the nodes
r.start()

# Grab the insert embedding topic
r.config.KAFKA_TOPICS["INSERT_EMBEDDING_TOPIC"]

# Sleep for data to flow
time.sleep(20)
# Create a client
client = r.get_client()
# Async request doc
client.request_documents("Whats hackernews top app?", 5)
# Async read response
pprint(client.parse_response())
r.stop()

