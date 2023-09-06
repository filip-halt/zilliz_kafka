import time
import json
import requests
from confluent_kafka import Producer
import json
import logging
import sys
import time
from copy import deepcopy
from threading import Event, Thread

import milvuskafka.values as values
from milvuskafka.datatypes import HackerNewsPost

logger = logging.getLogger("KafkaInsertLogger")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)


class HackerNewsParse:
    def __init__(self):
        # Kafka configs
        self.kafka_producer_config = values.KAFKA_DEFAULT_CONFIGS
        self.kafka_consumer_config = deepcopy(values.KAFKA_DEFAULT_CONFIGS)
        self.kafka_consumer_config.update({            
            "enable.auto.commit": False,
            'group.id': "MilvusSearch_Consumers",
            'auto.offset.reset': 'earliest'
        })

        # Kafka producer for query responses 
        # self.producer = Producer(self.kafka_producer_config)
    
    def start(self):
        # Start listening for new articles
        self.end_event = Event()
        self.run_thread = Thread(target=self.run, args=(self.end_event, ))
        self.run_thread.start()
    
    def stop(self):
        # Stop loop and wait for join
        self.end_event.set()
        self.run_thread.join()
    
    def run(self, stop_flag: Event):
        logger.debug("Started HackerNewsParse run() loop")
        # Continue running thread while stop_flag isnt set
        while not stop_flag.is_set():
            post_ids = self.get_new_hacker_news_posts()
            logger.debug(f"There are {len(post_ids)} new posts")
            for post_id in post_ids:
                post_url = f"https://hacker-news.firebaseio.com/v0/item/{post_id}.json"
                response = requests.get(post_url)
                if response.status_code == 200:
                    try:
                        post_data = HackerNewsPost(**json.loads(response.text))
                    except Exception:
                        print(json.loads(response.text))
                        exit()
                    self.respond(post_data)
                    
            time.sleep(values.HACKER_NEWS_PARSE_SLEEP)

        
    
    def respond(self, post: HackerNewsPost):
        self.producer.produce(
            topic=values.KAFKA_TOPICS["HACKERNEWS_PARSED_TOPIC"],
            value=json.dumps(post.model_dump(exclude_none=True))
        )
        logger.debug(f"Post with ID {post.id} was produced")


    def get_new_hacker_news_posts(self):
        response = requests.get(values.HACKER_NEWS_API_URL)
        if response.status_code == 200:
            post_ids = json.loads(response.text)
            return post_ids
        else:
            print("Failed to fetch Hacker News posts.")
            return []


if __name__ == '__main__':
    s = HackerNewsParse()
    s.start()
    time.sleep(1)
    s.stop()