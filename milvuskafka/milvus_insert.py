import json
import logging
import sys
from copy import deepcopy
from threading import Event, Thread

from confluent_kafka import Consumer
from pymilvus import MilvusClient
from milvuskafka.config import Configuration

from milvuskafka.datatypes import MilvusDocument

logger = logging.getLogger("logger")


class MilvusInsert:
    def __init__(self, config: Configuration):
        self.config = config
        # Milvus client for operation on the Milvus cluster, assumes that collection made and loaded
        self.milvus_client = MilvusClient(
            uri=self.config.MILVUS_URI, token=self.config.MILVUS_TOKEN
        )
        self.kafka_consumer_config = deepcopy(self.config.KAFKA_BASE_CONFIGS)
        self.kafka_consumer_config.update(
            {
                "enable.auto.commit": False,
                "group.id": "MilvusInsert_Consumers",
                "auto.offset.reset": "earliest",
            }
        )
        # Kafka consumer on predifiend topic for insert requests
        self.consumer = Consumer(self.kafka_consumer_config)
        self.consumer.subscribe([self.config.KAFKA_TOPICS["INSERT_REQUEST_TOPIC"]])

    def start(self):
        # Start listening to inserts
        self.end_event = Event()
        self.run_thread = Thread(target=self.run, args=(self.end_event,))
        self.run_thread.start()

    def stop(self):
        # Trigger end and wait for join
        self.end_event.set()
        self.run_thread.join()

    def run(self, stop_flag: Event):
        logger.debug("Started MilvusInsert run() loop")
        # Continue running thread while stop_flag isnt set
        while not stop_flag.is_set():
            # Poll for new message, non-blocking in order for event flag to work
            msg = self.consumer.poll(timeout=self.config.KAKFA_POLL_TIMEOUT)
            # If a message was caught, process it
            if msg is not None:
                try:
                    insert_vals = MilvusDocument(**json.loads(msg.value()))
                    self.insert(insert_vals)
                except Exception as e:
                    logger.debug(f"Failed to insert: {insert_vals.chunk_id}, {e}")
                # Commit msg
                # self.consumer.commit(msg)
        logger.debug("Exiting MilvusInsert run() loop")
        return

    def insert(self, insert_val: MilvusDocument):
        logger.debug(
            "Insert request recieved for chunk_id: %s", insert_val.chunk_id
        )
        # Convert data to dict
        data = insert_val.model_dump(exclude_none=True)
        # Insert data into Milvus
        self.milvus_client.insert(
            collection_name=self.config.MILVUS_COLLECTION,
            data=[data],
        )
        logger.debug("Inserted chunk_id: %s", insert_val.chunk_id)
        return
