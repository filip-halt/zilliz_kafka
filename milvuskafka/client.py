from copy import deepcopy
import json
import uuid
from milvuskafka.config import Configuration
from confluent_kafka import Consumer, Producer

from milvuskafka.datatypes import MilvusSearchResponse



class Client():
    def __init__(self, config: Configuration):
        self.config = config

        self.kafka_producer_config = config.KAFKA_BASE_CONFIGS
        self.kafka_consumer_config = deepcopy(config.KAFKA_BASE_CONFIGS)
        self.kafka_consumer_config.update(
            {
                "enable.auto.commit": False,
                "group.id": "Client_Consumers",
                "auto.offset.reset": "earliest",
            }
        )

        self.consumer = Consumer(self.kafka_consumer_config)
        self.consumer.subscribe([config.KAFKA_TOPICS["SEARCH_RESPONSE_TOPIC"]])

        self.producer = Producer(self.kafka_producer_config)

    def request_documents(self, prompt: str, top_k: int) -> dict:
        request = {
            "query_id": uuid.uuid4().hex,
            "text": prompt,
            "top_k": top_k,
        }

        self.producer.produce(
            topic=self.config.KAFKA_TOPICS["REQUEST_TOPIC"],
            key="search",
            value=json.dumps(request),
        )
        self.producer.flush()
    
    def parse_response(self) -> MilvusSearchResponse:
        msg = self.consumer.poll()
        search_vals = MilvusSearchResponse(**json.loads(msg.value()))
        self.consumer.commit(msg)
        return search_vals.model_dump()
