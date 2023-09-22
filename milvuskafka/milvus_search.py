import json
import logging
import sys
from copy import deepcopy
from threading import Event, Thread

from confluent_kafka import Consumer, Producer
from pymilvus import MilvusClient
from milvuskafka.config import Configuration

from milvuskafka.datatypes import (
    MilvusDocument,
    MilvusSearchRequest,
    MilvusSearchResponse,
)

logger = logging.getLogger("KafkaSearchLogger")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)


class MilvusSearch:
    def __init__(self, config: Configuration):
        self.config = config
        # Milvus client for operation on the Milvus cluster, assumes that collection made and loaded
        self.milvus_client = MilvusClient(
            uri=config.MILVUS_URI, token=config.MILVUS_TOKEN
        )
        # Kafka configs
        self.kafka_producer_config = config.KAFKA_BASE_CONFIGS
        self.kafka_consumer_config = deepcopy(config.KAFKA_BASE_CONFIGS)
        self.kafka_consumer_config.update(
            {
                "enable.auto.commit": False,
                "group.id": "MilvusSearch_Consumers",
                "auto.offset.reset": "earliest",
            }
        )

        # Kafka consumer on predifiend topic for query requests
        self.consumer = Consumer(self.kafka_consumer_config)
        self.consumer.subscribe([config.KAFKA_TOPICS["SEARCH_REQUEST_TOPIC"]])

        # Kafka producer for query responses
        self.producer = Producer(self.kafka_producer_config)

    def start(self):
        # Start listening for search requests
        self.end_event = Event()
        self.run_thread = Thread(target=self.run, args=(self.end_event,))
        self.run_thread.start()

    def stop(self):
        # Stop loop and wait for join
        self.end_event.set()
        self.run_thread.join()

    def run(self, stop_flag: Event):
        logger.debug("Started MilvusSearch run() loop")
        # Continue running thread while stop_flag isnt set
        while not stop_flag.is_set():
            # Poll for new message, non-blocking in order for event flag to work
            msg = self.consumer.poll(timeout=self.config.KAKFA_POLL_TIMEOUT)
            # If a message was caught, process it
            if msg is not None:
                try:
                    search_vals = MilvusSearchRequest(**json.loads(msg.value()))
                    # Get search response
                    res = self.search(search_vals)
                    # Produce the results
                    self.respond(res)
                except Exception as e:
                    logger.debug(f"Failed to search: {search_vals.query_id}, {e}")
                # Commit that the message was processed
                self.consumer.commit(msg)
        # Flush producer on finish
        self.producer.flush()
        logger.debug("Exiting MilvusSearch run() loop")
        return

    def search(self, search_vals: MilvusSearchRequest):
        # Search the collection for the given embedding
        logger.debug("Searching query_id: %s", search_vals.query_id)
        res = self.milvus_client.search(
            collection_name=self.config.MILVUS_COLLECTION,
            data=[search_vals.embedding],
            limit=search_vals.top_k,
            output_fields=["*"],
        )
        # Convert search results list of MilvusDocuments
        search_res = []
        for hit in res[0]:
            search_res.append(
                MilvusDocument(
                    distance=hit["distance"],
                    **hit["entity"],
                )
            )
        logger.debug(
            "Search on query_id: %s found %d results",
            search_vals.query_id,
            len(search_res),
        )
        # Convert to MilvusSearchResponse and return
        return MilvusSearchResponse(query_id=search_vals.query_id, results=search_res, text=search_vals.text)

    def respond(self, respond_vals: MilvusSearchResponse):
        # Send the results back through the desired topic
        self.producer.produce(
            topic=self.config.KAFKA_TOPICS["SEARCH_RESPONSE_TOPIC"],
            value=json.dumps(respond_vals.model_dump(exclude_none=True)),
        )
        # self.producer.flush()
        logger.debug("Search on query_id: %s sent result back", respond_vals.query_id)
