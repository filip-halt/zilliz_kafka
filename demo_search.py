from pprint import pprint
import time
from milvuskafka.runner import Runner


client = Runner.get_client("config.yaml")
q = input("What is your question?")
client.request_documents(q, 5)
while True:
    # Async read response
    pprint(client.parse_response())
