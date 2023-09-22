from pprint import pprint
import time
from milvuskafka.runner import Runner

r = Runner("config.yaml")
# Create the Milvus Collection and Kafka Topics
r.setup(overwrite=True)
# Start the nodes
# These are going to be hardcoded articles that have good RAG responses.
r.hn_runner.post_specific_ids([
    "37583593", # Does compression have any cool use cases?
    "37601297", # Is it safe to be a scientist?
    "37602239", # Is there carbon on Europa?
])
r.start()

# Run for x amount of time.
time.sleep(600)

r.stop()

