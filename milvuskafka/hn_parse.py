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
from milvuskafka.setup_services import setup_milvus

import milvuskafka.config as config
from milvuskafka.datatypes import HackerNewsPost
from pymilvus import MilvusClient

logger = logging.getLogger("HNParserLogger")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)


class HackerNewsParse:
    def __init__(self,  config: config.Configuration):
        # Kafka configs
        self.config = config

        self.kafka_producer_config = self.config.KAFKA_BASE_CONFIGS
        self.milvus_client = MilvusClient(
            uri=self.config.MILVUS_URI, token=self.config.MILVUS_TOKEN
        )

        # Kafka producer for new HN posts
        self.producer = Producer(self.kafka_producer_config)

    def start(self):
        # Start listening for new articles
        self.end_event = Event()
        self.run_thread = Thread(target=self.run, args=(self.end_event,))
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
                # Grab post info for each post id
                post_url = f"https://hacker-news.firebaseio.com/v0/item/{post_id}.json"
                response = requests.get(post_url)
                if response.status_code == 200:
                    # If invalid data, skip it and continue loop
                    try:
                        post_data = HackerNewsPost(**json.loads(response.text))
                    except Exception:
                        continue
                    # Respond with post info if valid
                    self.respond(post_data)
            logger.debug(
                f"Sleeping for {self.config.HACKER_NEWS_PARSE_SLEEP} seconds before next batch"
            )
            self.producer.flush()
            time.sleep(self.config.HACKER_NEWS_PARSE_SLEEP)
        # Flush producer on finish
        self.producer.flush()
        logger.debug("Exiting Producer run() loop")

    def respond(self, post: HackerNewsPost):
        # Only send the post if it doesnt exist already in milvus
        if not self.post_exists(post):
            self.producer.produce(
                topic=self.config.KAFKA_TOPICS["INSERT_EMBEDDING_TOPIC"],
                value=json.dumps(post.model_dump(exclude_none=True)),
            )
            logger.debug(f"Post with ID {post.id} was produced")
            # self.producer.flush()
        else:
            logger.debug(f"Post with ID {post.id} already exists, skipped")

    def post_exists(self, post: HackerNewsPost):
        # Query for the post to check if it already exists within the collection
        expr = f"id == {post.id}"
        res = self.milvus_client.query(
            self.config.MILVUS_COLLECTION, filter=expr, output_fields=["id"]
        )
        return len(res) != 0

    def get_new_hacker_news_posts(self):
        response = requests.get(self.config.HACKER_NEWS_API_URL)
        if response.status_code == 200:
            post_ids = json.loads(response.text)
            return post_ids
        else:
            logger.debug("Failed to fetch Hacker News posts.")
            return []
    
    def post_specific_ids(self, list_of_ids):
        for post_id in list_of_ids:
            # Grab post info for each post id
            post_url = f"https://hacker-news.firebaseio.com/v0/item/{post_id}.json"
            response = requests.get(post_url)
            if response.status_code == 200:
                # If invalid data, skip it and continue loop
                try:
                    post_data = HackerNewsPost(**json.loads(response.text))
                except Exception:
                    continue
                # Respond with post info if valid
                self.respond(post_data)
        # Flush producer on finish
        self.producer.flush()
