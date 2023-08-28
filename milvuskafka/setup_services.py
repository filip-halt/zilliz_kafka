import time
from confluent_kafka.admin import AdminClient, NewTopic
from pymilvus import MilvusClient, utility
import milvuskafka.values as values

def setup_milvus(
    overwrite=True
):
    milvus_client = MilvusClient(uri=values.MILVUS_URI, token=values.MILVUS_TOKEN)

    if values.MILVUS_COLLECTION in milvus_client.list_collections() and overwrite:
        milvus_client.drop_collection(values.MILVUS_COLLECTION)

    if values.MILVUS_COLLECTION not in milvus_client.list_collections():
        milvus_client.create_collection(
            collection_name=values.MILVUS_COLLECTION,
            dimension=values.MILVUS_DIM,
            primary_field_name="chunk_id",
            id_type = "str",
            max_length = 65_000,
            vector_field_name="embedding",
        )

def setup_kafka(overwrite=True):
    admin = AdminClient(values.KAFKA_DEFAULT_CONFIGS)
    if overwrite:
        try:
            fs = admin.delete_topics(list(values.KAFKA_TOPICS.values()))
            for _, f in fs.items():
                f.result()
        except:
            pass
            
    new_topics = [NewTopic(topic, num_partitions=1, replication_factor=values.KAFKA_REPLICATION_FACTOR) for topic in list(values.KAFKA_TOPICS.values())]
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

if __name__ == "__main__":
    # setup_milvus(dim=3, overwrite=True)
    setup_kafka()