from milvuskafka.milvus_insert import MilvusInsert
from milvuskafka.milvus_search import MilvusSearch
from milvuskafka.hn_parse import HackerNewsParse
from milvuskafka.embedder import Embedder
from milvuskafka.setup_services import setup_kafka, setup_milvus
from milvuskafka.config import Configuration


class Runner:
    def __init__(self, config_yaml_path: str = None):
        self.config = Configuration(config_yaml_path)
        self.insert_runner = MilvusInsert(self.config)
        self.search_runner = MilvusSearch(self.config)
        self.hn_runner = HackerNewsParse(self.config)
        self.embedder = Embedder(self.config)

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
        setup_milvus(self.config, overwrite)
        setup_kafka(self.config, overwrite)
