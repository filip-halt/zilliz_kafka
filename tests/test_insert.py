from copy import deepcopy
import json
import time
from typing import Tuple

import pytest
from confluent_kafka import Producer

import milvuskafka.values as values
from milvuskafka.datatypes import MilvusDocument, MilvusInsertRequest
from milvuskafka.milvus_insert import MilvusInsert
from milvuskafka.setup_services import setup_kafka, setup_milvus


@pytest.fixture
def runner_and_producer():
    values.MILVUS_DIM = 3
    setup_kafka()
    setup_milvus()
    insert_runner = MilvusInsert()
    insert_runner.start()
    kafka_producer_config = deepcopy(values.KAFKA_DEFAULT_CONFIGS)
    kafka_producer_config.update(
        {
            "queue.buffering.max.ms": 1,
        }
    )
    producer = Producer(kafka_producer_config)
    yield insert_runner, producer
    insert_runner.stop()


def test_input(runner_and_producer: Tuple[MilvusInsert, Producer]):
    r, producer = runner_and_producer
    test_doc = MilvusDocument(
        chunk_id="1",
        doc_id="1",
        chunk="All the dogs are happy",
        title="blah",
        by="blah",
        embedding=[0, 0, 0],
    )
    test_insert = MilvusInsertRequest(insert_id="1 id", doc=test_doc)
    producer.produce(
        topic=values.KAFKA_TOPICS["INSERT_REQUEST_TOPIC"],
        key="",
        value=json.dumps(test_insert.model_dump()),
    )
    producer.flush()
    # Need to sleep to let values flow
    time.sleep(15)
    r.milvus_client.flush(values.MILVUS_COLLECTION)
    res = r.milvus_client.query(values.MILVUS_COLLECTION, 'chunk_id == "1"')
    assert len(res) == 1
    assert res[0]["chunk_id"] == "1"
