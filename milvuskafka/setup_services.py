import time
from confluent_kafka.admin import AdminClient, NewTopic
from pymilvus import MilvusClient
import milvuskafka.config as config


def setup_milvus(overwrite=True):
    milvus_client = MilvusClient(uri=config.MILVUS_URI, token=config.MILVUS_TOKEN)

    if config.MILVUS_COLLECTION in milvus_client.list_collections() and overwrite:
        milvus_client.drop_collection(config.MILVUS_COLLECTION)

    if config.MILVUS_COLLECTION not in milvus_client.list_collections():
        milvus_client.create_collection(
            collection_name=config.MILVUS_COLLECTION,
            dimension=config.MILVUS_DIM,
            primary_field_name="chunk_id",
            id_type="str",
            max_length=65_000,
            vector_field_name="embedding",
            consistency_level="Strong",
        )


def setup_kafka(overwrite=True):
    admin = AdminClient(config.KAFKA_DEFAULT_CONFIGS)
    if overwrite:
        try:
            fs = admin.delete_topics(list(config.KAFKA_TOPICS.values()))
            for _, f in fs.items():
                f.result()
        except:
            pass

    new_topics = [
        NewTopic(
            topic, num_partitions=1, replication_factor=config.KAFKA_REPLICATION_FACTOR
        )
        for topic in list(config.KAFKA_TOPICS.values())
    ]
    # TODO: Retry mechanism for when it is in deletion state
    time.sleep(1)
    fs = admin.create_topics(new_topics)

    # Wait for each operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} created".format(topic))
        except Exception as e:
            print("Failed to create topic {}: {}".format(topic, e))

