from copy import deepcopy
import json
import time
from typing import Tuple

import pytest
from confluent_kafka import Producer, Consumer
from milvuskafka.config import Configuration

from milvuskafka.datatypes import (
    MilvusDocument,
    SearchRequest,
    HackerNewsPost,
    MilvusSearchRequest
)
from milvuskafka.embedder import Embedder
from milvuskafka.setup_services import setup_kafka, setup_milvus


@pytest.fixture
def runner_and_sinks():
    config = Configuration()
    config.KAFKA_BASE_CONFIGS = {
        "bootstrap.servers": "localhost:9094",
        "queue.buffering.max.ms": "10"
    }
    config.MILVUS_URI = "http://localhost:19530"
    config.MILVUS_TOKEN = ""
    setup_kafka(config)
    # setup_milvus()
    embed_runner = Embedder(config)
    embed_runner.start()
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
            "group.id": "Test_Embedder_Consumer",
            "auto.offset.reset": "earliest",
        }
    )

    consumer_insert = Consumer(kafka_consumer_config)
    consumer_search = Consumer(kafka_consumer_config)
    consumer_insert.subscribe([config.KAFKA_TOPICS["INSERT_REQUEST_TOPIC"]])
    consumer_search.subscribe([config.KAFKA_TOPICS["SEARCH_REQUEST_TOPIC"]])

    yield embed_runner, producer, consumer_search, consumer_insert, config
    embed_runner.stop()

def test_embedder(runner_and_sinks: Tuple[Embedder, Producer, Consumer, Consumer, Configuration]):

    r, producer, consumer_search, consumer_insert, config = runner_and_sinks
    # Test all the different hn posts and search types
    test_search = SearchRequest(
        query_id="blah",
        text="Lorem ipsum dolor sit amet, consectetur adipiscing elit. Mauris vehicula ligula ac nibh tincidunt, ut pharetra augue efficitur. ",
        top_k=1,
    )
    # Using website that hasnt changed since start of internet
    test_hnpost_url = HackerNewsPost(
        title="lorem",
        by="ipsum",
        type="blah",
        id=10101,
        url="http://info.cern.ch/hypertext/WWW/TheProject.html"
    )
    test_hnpost_text = HackerNewsPost(
        title="lorem1",
        by="ipsum1",
        type="blah1",
        id=10102,
        text="Lorem ipsum dolor sit amet, consectetur adipiscing elit. Mauris vehicula ligula ac nibh tincidunt, ut pharetra augue efficitur. ",
    )
    producer.produce(
        topic=config.KAFKA_TOPICS["REQUEST_TOPIC"],
        key="insert",
        value=json.dumps(test_hnpost_text.model_dump()),
    )
    producer.produce(
        topic=config.KAFKA_TOPICS["REQUEST_TOPIC"],
        key="insert",
        value=json.dumps(test_hnpost_url.model_dump()),
    )
    producer.produce(
        topic=config.KAFKA_TOPICS["REQUEST_TOPIC"],
        key="search",
        value=json.dumps(test_search.model_dump()),
    )
    producer.flush()
    # Need to sleep to let values flow
    time.sleep(10)
    r.producer.flush()
    # Need to sleep to let values flow
    time.sleep(10)
    keep_running = set()
    count = 0
    # Consume until both returning null
    while len(keep_running) < 2:
        msg = consumer_search.poll(timeout=3)
        if msg is None:
            keep_running.add(0)
        else:
            MilvusSearchRequest(**json.loads(msg.value()))
            count += 1
        msg = consumer_insert.poll(timeout=3)
        if msg is None:
            keep_running.add(1)
        else:
            MilvusDocument(**json.loads(msg.value()))
            count+=1
    assert count == 3
 
