import json
import logging
import sys
import time
from copy import deepcopy
from threading import Event, Thread
from typing import List

from confluent_kafka import Consumer, Producer
from pymilvus import MilvusClient

import milvuskafka.values as values
from milvuskafka.datatypes import (MilvusDocument, HackerNewsPost, SearchRequest, MilvusSearchRequest)
from langchain.document_loaders import UnstructuredURLLoader  # pylint: disable=C0415
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.text_splitter import RecursiveCharacterTextSplitter


logger = logging.getLogger("KafkaEmbedderLog")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)


class Embedder:
    def __init__(self):
        # Kafka configs
        self.kafka_producer_config = values.KAFKA_DEFAULT_CONFIGS
        self.kafka_consumer_config = deepcopy(values.KAFKA_DEFAULT_CONFIGS)
        self.kafka_consumer_config.update({            
            "enable.auto.commit": False,
            'group.id': "Request_Consumers",
            'auto.offset.reset': 'earliest'
        })

        # Kafka consumer on request topics, can include search and insert requests
        self.consumer = Consumer(self.kafka_consumer_config)
        self.consumer.subscribe([values.KAFKA_TOPICS["REQUST_TOPIC"]])

        # Producer for both insert and search requests
        self.producer = Producer(self.kafka_producer_config)

        # SentenceTransformers as the embedding model
        self.embedder = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")
        # Using a basic character text splitter for chunking
        self.text_splitter = RecursiveCharacterTextSplitter()

    def start(self):
        # Start listening for requests
        self.end_event = Event()
        self.run_thread = Thread(target=self.run, args=(self.end_event, ))
        self.run_thread.start()
    
    def stop(self):
        # Stop loop and wait for join
        self.end_event.set()
        self.run_thread.join()
    
    def run(self, stop_flag: Event):
        logger.debug("Started Embedding run() loop")
        # Continue running thread while stop_flag isnt set
        while not stop_flag.is_set():
            # Poll for new message, non-blocking in order for event flag to work
            msg = self.consumer.poll(timeout=values.KAKFA_POLL_TIMEOUT)
            # If a message was caught, process it
            if msg is not None:
                # If the msg key is an insert, we load the HackerNewsPost
                if msg.key() == "insert":
                    post = HackerNewsPost(**json.loads(msg.value()))
                    # Embed the post into a list of MilvusDocs, each containing a chunk
                    res = self.embed_post(post)
                    # Produce the result to the insert channel
                    self.respond_insert(res)
                elif msg.key() == "search":
                    post = SearchRequest(**json.loads(msg.value()))
                    # Get search response
                    res = self.embed_search(post)
                    # Produce the results
                    self.respond_search(res)

        # Commit that the message was processed
        self.consumer.commit(msg)
        # Flush producer on finish
        self.producer.flush()   
        logger.debug("Exiting MilvusSearch run() loop")
        return

    def embed_search(self, search: SearchRequest) -> MilvusSearchRequest:
        # Embed the search text
        embedding = self.embedder.embed_query(search.text)
        # Format request to the correct pydantic
        search_request = MilvusSearchRequest(
            query_id=search.query_id,
            embedding=embedding,
            top_k=search.top_k
        )
        return search_request

    def embed_post(self, post: HackerNewsPost) -> List[MilvusDocument]:
        milvus_docs: List[MilvusDocument] = []
        # First check if it is a URL post, if so we ignore its text
        if post.url is not None:
            # Load the data found in the url
            loader = UnstructuredURLLoader(urls=[post.url])
            # Chunk the data into a list of docs (container for text string)
            docs = loader.load_and_split(text_splitter=self.text_splitter)
            # Embed each chunk and create a MilvusDocument for each
            for i, doc in enumerate(docs):
                # If the content is there, embed it
                if doc.get("page_content", None) is not None:
                    embedding = self.embedder.embed_query(doc["page_content"])
                    new_doc = MilvusDocument(
                        chunk_id=str(post.id) + "_" + str(i),  # Chunk id is the original id + chunk number
                        doc_id = str(post.id), # Convert the original id to string for Milvus
                        chunk=doc["page_content"], # Chunk is the original text of the chunk
                        title=post.title,
                        by=post.by,
                        url=post.url,
                        embedding=embedding,
                    )
                    milvus_docs.append(new_doc)

        # If there is no URL, its a text post, requiring only the text inside to be embedded
        elif post.text is not None:
            # Split the text of the post and embed each
            docs = self.text_splitter.split_text(post.text)
            for i, doc in enumerate(docs):
                embedding = self.embedder.embed_query(doc)
                new_doc = MilvusDocument(
                    chunk_id=str(post.id) + "_" + str(i), 
                    doc_id = str(post.id),
                    chunk=doc["page_content"],
                    title=post.title,
                    by=post.by,
                    embedding=embedding,
                )
                milvus_docs.append(new_doc)

        return milvus_docs

    def respond_insert(self, respond_vals: List[MilvusDocument]):
        # Produce the milvus documents to the Insert topic
        for x in respond_vals:
            self.producer.produce(
                topic=values.KAFKA_TOPICS["INSERT_REQUEST_TOPIC"],
                value=json.dumps(x.model_dump(exclude_none=True))
            )
            logger.debug("Insert for document: %s sent to insert topic", x.doc_id)
    
    def respond_search(self, respond_val: MilvusSearchRequest):
        # Produce the search reqeusts to the search topic
        self.producer.produce(
            topic=values.KAFKA_TOPICS["SEARCH_REQUEST_TOPIC"],
            value=json.dumps(respond_val.model_dump(exclude_none=True))
        )
        logger.debug("Search for document: %s sent to search topic", respond_val.query_id)
