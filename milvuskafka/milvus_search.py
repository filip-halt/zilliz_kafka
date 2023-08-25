import json
import logging
import sys
import time
from threading import Event, Thread

from confluent_kafka import Consumer, Producer
from pymilvus import MilvusClient

import milvuskafka.values as values
from milvuskafka.datatypes import (MilvusDocument, MilvusSearchRequest,
                       MilvusSearchResponse)

logger = logging.getLogger("KafkaSearchLogger")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)


class MilvusSearch:
    def __init__(self):
        # Milvus client for operation on the Milvus cluster, assumes that collection made and loaded
        self.milvus_client = MilvusClient(
            uri=values.MILVUS_URI,
            token=values.MILVUS_TOKEN
        )
        # Kafka configs
        self.kafka_producer_config = {
            "bootstrap.servers": values.KAFKA_ADDRESS,
        }
        self.kafka_consumer_config = {
            "bootstrap.servers": values.KAFKA_ADDRESS,
            "enable.auto.commit": False,
            'group.id': "MilvusSearch_Consumers",
            'auto.offset.reset': 'earliest'
        }

        # Kafka consumer on predifiend topic for query requests
        self.consumer = Consumer(self.kafka_consumer_config)
        self.consumer.subscribe([values.KAFKA_TOPICS["SEARCH_CONSUMER_TOPIC"]])

        # Kafka producer for query responses 
        self.producer = Producer(self.kafka_producer_config)
    
    def start(self):
        # Start listening for search requests
        self.end_event = Event()
        self.run_thread = Thread(target=self.run, args=(self.end_event, ))
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
            msg = self.consumer.poll(timeout=values.KAKFA_POLL_TIMEOUT)
            # If a message was caught, process it
            if msg is not None:
                search_vals = MilvusSearchRequest(**json.loads(msg.value()))
                # Get search response
                res = self.search(search_vals)
                # Produce the results
                self.respond(res)
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
            collection_name=values.MILVUS_COLLECTION,
            data=[search_vals.embedding],
            limit=search_vals.top_k,
            output_fields=["*"]
        )
        # Convert search results list of MilvusDocuments
        search_res = []
        for hit in res[0]:
            search_res.append(
                MilvusDocument(
                    distance = hit["distance"],
                    **hit["entity"],
                )
            )
        logger.debug("Search on query_id: %s found %d results", search_vals.query_id, len(search_res))
        # Convert to MilvusSearchResponse and return
        return MilvusSearchResponse(
            query_id=search_vals.query_id,
            results = search_res
        )

    def respond(self, respond_vals: MilvusSearchResponse):
        # Send the results back through the desired topic
        self.producer.produce(
            topic=values.KAFKA_TOPICS["SEARCH_PRODUCER_TOPIC"],
            value=json.dumps(respond_vals.model_dump(exclude_none=True))
        )
        logger.debug("Search on query_id: %s sent result back", respond_vals.query_id)
