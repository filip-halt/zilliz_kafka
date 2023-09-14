from copy import deepcopy
import json
import time
from typing import Tuple

import pytest
from confluent_kafka import Producer, Consumer

import milvuskafka.values as values
from milvuskafka.datatypes import (
    MilvusDocument,
    MilvusSearchRequest,
    MilvusSearchResponse,
)
from milvuskafka.milvus_search import MilvusSearch
from milvuskafka.setup_services import setup_kafka, setup_milvus


@pytest.fixture
def runner_and_producer_consumer():
    values.MILVUS_DIM = 3
    setup_kafka()
    setup_milvus()
    search_runner = MilvusSearch()
    search_runner.start()
    kafka_producer_config = deepcopy(values.KAFKA_DEFAULT_CONFIGS)
    kafka_producer_config.update(
        {
            "queue.buffering.max.ms": 1,
        }
    )
    producer = Producer(kafka_producer_config)
    kafka_consumer_config = deepcopy(values.KAFKA_DEFAULT_CONFIGS)
    kafka_consumer_config.update(
        {
            "enable.auto.commit": False,
            "group.id": "Test_Search_Consumer",
            "auto.offset.reset": "earliest",
        }
    )

    consumer = Consumer(kafka_consumer_config)
    consumer.subscribe([values.KAFKA_TOPICS["SEARCH_RESPONSE_TOPIC"]])
    yield search_runner, producer, consumer
    search_runner.stop()


def test_search(runner_and_producer_consumer: Tuple[MilvusSearch, Producer, Consumer]):
    r, producer, consumer = runner_and_producer_consumer
    r.milvus_client.insert(
        values.MILVUS_COLLECTION,
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
        embedding=[0, 0, 0],
        top_k=1,
    )
    producer.produce(
        topic=values.KAFKA_TOPICS["SEARCH_REQUEST_TOPIC"],
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
