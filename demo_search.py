from pprint import pprint
import time
from milvuskafka.runner import Runner


client = Runner.get_client("config.yaml")

while True:
    q = input("What is your question?")
    client.request_documents(q, 5)
    # Async read response
    pprint(client.parse_response())
