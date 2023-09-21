import json
import logging
import sys
from copy import deepcopy
from threading import Event, Thread
from typing import List

from confluent_kafka import Consumer, Producer

from milvuskafka.config import Configuration
from milvuskafka.datatypes import (
    MilvusDocument,
    HackerNewsPost,
    SearchRequest,
    MilvusSearchRequest,
)
from langchain.document_loaders import UnstructuredURLLoader  # pylint: disable=C0415
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.text_splitter import RecursiveCharacterTextSplitter


logger = logging.getLogger("KafkaSearchEmbedderLog")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)


class EmbedderSearch:
    def __init__(self, config: Configuration):
        # Kafka configs
        self.config = config
        self.kafka_producer_config =  self.config.KAFKA_BASE_CONFIGS
        self.kafka_consumer_config = deepcopy(self.config.KAFKA_BASE_CONFIGS)
        self.kafka_consumer_config.update(
            {
                "enable.auto.commit": False,
                "group.id": "Request_Consumers",
                "auto.offset.reset": "earliest",
            }
        )

        # Kafka consumer on request topics, can include search and insert requests
        self.consumer = Consumer(self.kafka_consumer_config)
        self.consumer.subscribe([ self.config.KAFKA_TOPICS["SEARCH_EMBEDDING_TOPIC"]])

        # Producer for both insert and search requests
        self.producer = Producer(self.kafka_producer_config)

        # SentenceTransformers as the embedding model
        self.embedder = HuggingFaceEmbeddings(model_name=config.EMBEDDING_MODEL)
        # Using a basic character text splitter for chunking
        self.text_splitter = RecursiveCharacterTextSplitter()

    def start(self):
        # Start listening for requests
        self.end_event = Event()
        self.run_thread = Thread(target=self.run, args=(self.end_event,))
        self.run_thread.start()

    def stop(self):
        # Stop loop and wait for join
        self.end_event.set()
        self.run_thread.join()

    def run(self, stop_flag: Event):
        logger.debug("Started SearchEmbedder run() loop")
        # Continue running thread while stop_flag isnt set
        while not stop_flag.is_set():
            # Poll for new message, non-blocking in order for event flag to work
            msg = self.consumer.poll(timeout= self.config.KAKFA_POLL_TIMEOUT)
            # If a message was caught, process it
            if msg is not None:
                # Broad try except for now to skip faulty data
                try:
                    post = SearchRequest(**json.loads(msg.value()))
                    logger.debug(
                        f"Recieved search request with query_id: {post.query_id}"
                    )
                    # Get search response
                    res = self.embed_search(post)
                    # Produce the results
                    self.respond_search(res)
                except Exception as e:
                    logger.debug(f"Failed to embed search: {post.query_id}, {e}")
                # Commit that the message was processed
                self.consumer.commit(msg)
                    
        # Flush producer on finish
        self.producer.flush()
        logger.debug("Exiting SearchEmbedder run() loop")
        return

    def embed_search(self, search: SearchRequest) -> MilvusSearchRequest:
        # Embed the search text
        embedding = self.embedder.embed_query(search.text)
        # Format request to the correct pydantic
        search_request = MilvusSearchRequest(
            query_id=search.query_id, embedding=embedding, top_k=search.top_k, text=search.text
        )
        return search_request


    def respond_search(self, respond_val: MilvusSearchRequest):
        # Produce the search reqeusts to the search topic
        self.producer.produce(
            topic= self.config.KAFKA_TOPICS["SEARCH_REQUEST_TOPIC"],
            value=json.dumps(respond_val.model_dump(exclude_none=True)),
        )
        self.producer.flush()
        logger.debug(
            f"Search with query_id: {respond_val.query_id} sent to search topic"
        )
