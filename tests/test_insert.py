from copy import deepcopy
import json
import time
from typing import Tuple

import pytest
from confluent_kafka import Producer
from milvuskafka.config import Configuration

from milvuskafka.datatypes import MilvusDocument
from milvuskafka.milvus_insert import MilvusInsert
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
    insert_runner = MilvusInsert(config)
    insert_runner.start()
    kafka_producer_config = deepcopy(config.KAFKA_BASE_CONFIGS)
    kafka_producer_config.update(
        {
            "queue.buffering.max.ms": 1,
        }
    )
    producer = Producer(kafka_producer_config)
    yield insert_runner, producer, config
    insert_runner.stop()


def test_input(runner_and_sinks: Tuple[MilvusInsert, Producer, Configuration]):
    r, producer, config = runner_and_sinks
    test_doc = MilvusDocument(
        chunk_id="1",
        doc_id="1",
        chunk="All the dogs are happy",
        title="blah",
        by="blah",
        embedding=[0, 0, 0],
    )
    producer.produce(
        topic=config.KAFKA_TOPICS["INSERT_REQUEST_TOPIC"],
        key="",
        value=json.dumps(test_doc.model_dump()),
    )
    producer.flush()
    # Need to sleep to let values flow
    time.sleep(15)
    r.milvus_client.flush(config.MILVUS_COLLECTION)
    res = r.milvus_client.query(config.MILVUS_COLLECTION, 'chunk_id == "1"')
    assert len(res) == 1
    assert res[0]["chunk_id"] == "1"
