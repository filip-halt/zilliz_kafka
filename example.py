from pprint import pprint
import time
from milvuskafka.runner import Runner

r = Runner("config.yaml")
# Create the Milvus Collection and Kafka Topics
r.setup()
# Start the nodes
r.start()
# Sleep for data to flow
time.sleep(10)
# Create a client
client = r.get_client()
# Async request doc
client.request_documents("Whats hackernews top app?", 5)
# Async read response
pprint(client.parse_response())
r.stop()

