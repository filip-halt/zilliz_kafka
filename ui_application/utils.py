import json
import asyncio
import queue
import yaml
from confluent_kafka import Consumer, KafkaError
from milvuskafka.runner import Runner

# Load configuration from config.yaml
with open('../config.yaml', 'r') as yaml_file:
    config = yaml.safe_load(yaml_file)

topic = ['InsertEmbeddingRequest']

conf = {
    'bootstrap.servers': config['KAFKA_SERVER'],
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': config['KAFKA_USERNAME'],
    'sasl.password': config['KAFKA_PASSWORD'],
    'group.id': 'foo',
    'auto.offset.reset': 'smallest',
    'enable.auto.commit': True
}

# Create a Kafka consumer instance
consumer = Consumer(conf)

# Subscribe to the topic specified in the config
consumer.subscribe(topic)

runner = Runner("../config.yaml")
client = Runner.get_client("../config.yaml")


async def get_queue():
    global socket_queue
    return socket_queue

async def create_queue():
    global socket_queue
    socket_queue = asyncio.Queue()

async def send_question(prompt):
    client.request_documents(prompt, 5)

async def get_answers():
    while True:
        await asyncio.sleep(0.2)
        res = client.parse_response()
        if res is None:
            continue
        else:
            question = res["question"]
            response = res["response"]
            await socket_queue.put({"type": "answer", "data": f"Question: {question}\nAnswer: {response}"})

async def start_backfill():
    # Create the Milvus Collection and Kafka Topics
    runner.setup(overwrite=True)
    # Start the nodes
    # These are going to be hardcoded articles that have good RAG responses.
    runner.hn_runner.post_specific_ids([
        "37583593", # Does compression have any cool use cases?
        "37601297", # Is it safe to be a scientist?
        "37602239", # Is there carbon on Europa?
    ])
    runner.start()



async def get_kafka_messages():
    while True:
        await asyncio.sleep(0.2)
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            handle_kafka_error(msg)
        else:
            hn_result = process_kafka_message(msg)
            await socket_queue.put({"type": "post", "data": hn_result})
            # Add a small delay to avoid excessive CPU usage

def handle_kafka_error(msg):
    # Handle any errors that occurred while polling for messages
    if msg.error().code() == KafkaError._PARTITION_EOF:
        print(f'Reached end of partition {msg.partition()}')
    else:
        print(f'Error: {msg.error()}')  # Print the error message


def process_kafka_message(msg):
    """
    Process a Kafka message, remove 'type' and 'url' fields, and return a formatted string.

    Args:
        msg (KafkaMessage): The Kafka message to process.

    Returns:
        str: The formatted message.
    """
    try:
        message_value = msg.value().decode("utf-8")
        data = json.loads(message_value)

        # Create the hn_url
        hn_url = f"https://news.ycombinator.com/item?id={data.get('id', '')}"
        
        result = f"""Title: {data["title"]}\nAuthor: {data["by"]} \nSource: {hn_url}"""

        return result
    except Exception as e:
        print(f'Error parsing message: {str(e)}')


