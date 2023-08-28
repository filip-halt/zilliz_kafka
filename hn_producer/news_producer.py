import time
import json
import requests
from confluent_kafka import Producer

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost"
KAFKA_TOPIC = "hacker_news_posts"

# Hacker News API URL
HACKER_NEWS_API_URL = 'https://hacker-news.firebaseio.com/v0/newstories.json'


def create_kafka_topic():
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    topic = NewTopic(
        topic=TOPIC_NAME,
        num_partitions=NUM_PARTITIONS,
        replication_factor=REPLICATION_FACTOR
    )

    topic_list = [topic]

    # Create topics
    futures = admin_client.create_topics(topic_list)

    for topic, future in futures.items():
        try:
            future.result()
            print(f"Topic {topic} created.")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")


def get_new_hacker_news_posts():
    response = requests.get(HACKER_NEWS_API_URL)
    if response.status_code == 200:
        post_ids = json.loads(response.text)
        return post_ids
    else:
        print("Failed to fetch Hacker News posts.")
        return []


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def produce_new_posts(producer):
    while True:
        post_ids = get_new_hacker_news_posts()
        for post_id in post_ids:
            post_url = f"https://hacker-news.firebaseio.com/v0/item/{post_id}.json"
            response = requests.get(post_url)
            if response.status_code == 200:
                post_data = json.loads(response.text)
                producer.produce(KAFKA_TOPIC, value=post_data, callback=delivery_report)
                producer.flush()
                print("Emitted post to Kafka:", post_data['title'])
            else:
                print("Failed to fetch post:", post_id)
        
        time.sleep(60)


if __name__ == '__main__':
    create_kafka_topic()
    
    producer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "hacker-news-producer"
    }

    producer = Producer(producer_config)
    
    produce_new_posts(producer)
