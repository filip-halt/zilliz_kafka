from milvuskafka.milvus_insert import MilvusInsert
from milvuskafka.milvus_search import MilvusSearch
import milvuskafka.values as values
from milvuskafka.setup_services import setup_kafka, setup_milvus
class Runner:
    def __init__(self):
        self.insert_runner = MilvusInsert()
        self.search_runner = MilvusSearch()

    def start(self):
        self.insert_runner.run()
        self.search_runner.run()

    def stop(self):
        self.insert_runner.stop()
        self.search_runner.stop()
    
    def setup(self, overwrite = True):
        setup_milvus(overwrite)
        setup_kafka(overwrite)

