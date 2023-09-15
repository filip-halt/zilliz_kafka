import time
from milvuskafka.runner import Runner

r = Runner("path to config.yaml")
r.setup()
r.start()
time.sleep(100)
r.stop()