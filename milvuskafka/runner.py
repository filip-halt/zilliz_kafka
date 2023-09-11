from milvuskafka.milvus_insert import MilvusInsert
from milvuskafka.milvus_search import MilvusSearch
from milvuskafka.hn_parse import HackerNewsParse
from milvuskafka.embedder import Embedder
import milvuskafka.values as values
from milvuskafka.setup_services import setup_kafka, setup_milvus


class Runner:
    def __init__(self):
        self.insert_runner = MilvusInsert()
        self.search_runner = MilvusSearch()
        self.hn_runner = HackerNewsParse()
        self.embedder = Embedder()

    def start(self):
        self.insert_runner.run()
        self.search_runner.run()
        self.hn_runner.run()
        self.embedder.run()

    def stop(self):
        self.insert_runner.stop()
        self.search_runner.stop()
        self.hn_runner.stop()
        self.embedder.stop()

    def setup(self, overwrite=True):
        setup_milvus(overwrite)
        setup_kafka(overwrite)
