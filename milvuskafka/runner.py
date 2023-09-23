from milvuskafka.milvus_insert import MilvusInsert
from milvuskafka.milvus_search import MilvusSearch
from milvuskafka.hn_parse import HackerNewsParse
from milvuskafka.embedder_insert import EmbedderInsert
from milvuskafka.embedder_search import EmbedderSearch
from milvuskafka.setup_services import setup_kafka, setup_milvus
from milvuskafka.config import Configuration
from milvuskafka.client import Client
import logging
import sys

logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)

class Runner:
    def __init__(self, config_yaml_path: str = None):
        self.config = Configuration(config_yaml_path)
        self.insert_runner = MilvusInsert(self.config)
        self.search_runner = MilvusSearch(self.config)
        self.hn_runner = HackerNewsParse(self.config)
        self.embedder_search = EmbedderSearch(self.config)
        self.embedder_insert = EmbedderInsert(self.config)


    def start(self):
        self.insert_runner.start()
        self.search_runner.start()
        self.hn_runner.start()
        self.embedder_insert.start()
        self.embedder_search.start()

    def stop(self):
        self.hn_runner.stop()
        self.embedder_search.stop()
        self.embedder_insert.stop()
        self.insert_runner.stop()
        self.search_runner.stop()

    def setup(self, overwrite=True):
        setup_milvus(self.config, overwrite)
        setup_kafka(self.config, overwrite)

    @staticmethod
    def get_client(config_yaml_path: str = None):
        config = Configuration(config_yaml_path)
        return Client(config)
