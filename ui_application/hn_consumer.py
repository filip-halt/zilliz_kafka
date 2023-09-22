import json
import asyncio
import yaml
from confluent_kafka import Consumer, KafkaError

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
    'auto.offset.reset': 'smallest'
}

# Create a Kafka consumer instance
consumer = Consumer(conf)

# Subscribe to the topic specified in the config
consumer.subscribe(topic)


async def get_kafka_messages():
    global latest_message
    while True:
        msg = consumer.poll(30.0)

        if msg is None:
            continue

        if msg.error():
            handle_kafka_error(msg)
        else:
            hn_result = process_kafka_message(msg)
            await asyncio.sleep(0.1)  # Add a small delay to avoid excessive CPU usage
            return hn_result


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

        result = f'Title = {data["title"]} and is by {data["by"]} and can be found at {hn_url}'

        print(result)

        return result
    except Exception as e:
        print(f'Error parsing message: {str(e)}')


