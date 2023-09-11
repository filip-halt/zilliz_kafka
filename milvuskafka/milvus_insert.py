import json
import logging
import sys
import time
from copy import deepcopy
from threading import Event, Thread

from confluent_kafka import Consumer
from pymilvus import MilvusClient

import milvuskafka.values as values
from milvuskafka.datatypes import MilvusInsertRequest

logger = logging.getLogger("KafkaInsertLogger")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)


class MilvusInsert:
    def __init__(self):
        # Milvus client for operation on the Milvus cluster, assumes that collection made and loaded
        self.milvus_client = MilvusClient(
            uri=values.MILVUS_URI,
            token=values.MILVUS_TOKEN
        )
        self.kafka_consumer_config = deepcopy(values.KAFKA_DEFAULT_CONFIGS)
        self.kafka_consumer_config.update({            
            "enable.auto.commit": False,
            'group.id': "MilvusInsert_Consumers",
            'auto.offset.reset': 'earliest'
        })
        # Kafka consumer on predifiend topic for insert requests
        self.consumer = Consumer(self.kafka_consumer_config)
        self.consumer.subscribe([values.KAFKA_TOPICS["INSERT_REQUEST_TOPIC"]])

    def start(self):
        # Start listening to inserts
        self.end_event = Event()
        self.run_thread = Thread(target=self.run, args=(self.end_event, ))
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
            msg = self.consumer.poll(timeout=values.KAKFA_POLL_TIMEOUT)
            # If a message was caught, process it
            if msg is not None:
                insert_vals = MilvusInsertRequest(**json.loads(msg.value()))
                self.insert(insert_vals)
                # Commit msg
                self.consumer.commit(msg)
        logger.debug("Exiting MilvusInsert run() loop")
        return
                
    def insert(self, insert_vals: MilvusInsertRequest):
        logger.debug("Insert request recieved with insert_id: %s", insert_vals.insert_id)
        # Convert data to dict
        data = insert_vals.doc.model_dump(exclude_none=True)
        # Insert data into Milvus
        self.milvus_client.insert(
            collection_name=values.MILVUS_COLLECTION,
            data=[data],
        )
        logger.debug("Inserted insert_id: %s", insert_vals.insert_id)
        return