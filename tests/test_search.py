from copy import deepcopy
import json
import time
from typing import Tuple

import pytest
from confluent_kafka import Producer, Consumer
from milvuskafka.config import Configuration

from milvuskafka.datatypes import (
    MilvusSearchRequest,
    MilvusSearchResponse,
)
from milvuskafka.milvus_search import MilvusSearch
from milvuskafka.setup_services import setup_kafka, setup_milvus


@pytest.fixture
def runner_and_sinks():
    config = Configuration()
    config.EMBEDDING_DIM = 3
    config.KAFKA_BASE_CONFIGS = {
        "bootstrap.servers": "localhost:9094",
        "queue.buffering.max.ms": "10"
    }
    setup_kafka(config)
    setup_milvus(config)
    search_runner = MilvusSearch(config)
    search_runner.start()
    kafka_producer_config = deepcopy(config.KAFKA_BASE_CONFIGS)
    kafka_producer_config.update(
        {
            "queue.buffering.max.ms": 1,
        }
    )
    producer = Producer(kafka_producer_config)
    kafka_consumer_config = deepcopy(config.KAFKA_BASE_CONFIGS)
    kafka_consumer_config.update(
        {
            "enable.auto.commit": False,
            "group.id": "Test_Search_Consumer",
            "auto.offset.reset": "earliest",
        }
    )

    consumer = Consumer(kafka_consumer_config)
    consumer.subscribe([config.KAFKA_TOPICS["SEARCH_RESPONSE_TOPIC"]])
    yield search_runner, producer, consumer, config
    search_runner.stop()


def test_search(runner_and_sinks: Tuple[MilvusSearch, Producer, Consumer, Configuration]):
    r, producer, consumer, config = runner_and_sinks
    r.milvus_client.insert(
        config.MILVUS_COLLECTION,
        data=[
            {
                "chunk_id": "1",
                "doc_id": "1",
                "chunk": "123",
                "title": "blah",
                "by": "blah",
                "embedding": [0, 0, 0],
            }
        ],
    )
    test_search = MilvusSearchRequest(
        query_id="blah",
        text="blah",
        embedding=[0, 0, 0],
        top_k=1,
    )
    producer.produce(
        topic=config.KAFKA_TOPICS["SEARCH_REQUEST_TOPIC"],
        key="",
        value=json.dumps(test_search.model_dump()),
    )
    producer.flush()
    # Need to sleep to let values flow
    time.sleep(10)
    r.producer.flush()
    # Need to sleep to let values flow
    time.sleep(10)
    msg = consumer.poll(timeout=3)
    results = MilvusSearchResponse(**json.loads(msg.value()))
    assert results.results[0].chunk_id == "1"
