import json
import logging
import sys
from copy import deepcopy
from threading import Event, Thread
from typing import List

from confluent_kafka import Consumer, Producer

from milvuskafka.config import Configuration
from milvuskafka.datatypes import (
    MilvusDocument,
    HackerNewsPost,
    SearchRequest,
    MilvusSearchRequest,
)
from langchain.document_loaders import UnstructuredURLLoader  # pylint: disable=C0415
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.text_splitter import RecursiveCharacterTextSplitter


logger = logging.getLogger("logger")


class EmbedderInsert:
    def __init__(self, config: Configuration):
        # Kafka configs
        self.config = config
        self.kafka_producer_config =  self.config.KAFKA_BASE_CONFIGS
        self.kafka_consumer_config = deepcopy(self.config.KAFKA_BASE_CONFIGS)
        self.kafka_consumer_config.update(
            {
                "enable.auto.commit": False,
                "group.id": "InsertEmbedder_Consumers",
                "auto.offset.reset": "earliest",
            }
        )

        # Kafka consumer on request topics, can include search and insert requests
        self.consumer = Consumer(self.kafka_consumer_config)
        self.consumer.subscribe([ self.config.KAFKA_TOPICS["INSERT_EMBEDDING_TOPIC"]])

        # Producer for both insert and search requests
        self.producer = Producer(self.kafka_producer_config)

        # SentenceTransformers as the embedding model
        self.embedder = HuggingFaceEmbeddings(model_name=config.EMBEDDING_MODEL)
        # Using a basic character text splitter for chunking
        self.text_splitter = RecursiveCharacterTextSplitter()

    def start(self):
        # Start listening for requests
        self.end_event = Event()
        self.run_thread = Thread(target=self.run, args=(self.end_event,))
        self.run_thread.start()

    def stop(self):
        # Stop loop and wait for join
        self.end_event.set()
        self.run_thread.join()

    def run(self, stop_flag: Event):
        logger.debug("Started InsertEmbedder run() loop")
        # Continue running thread while stop_flag isnt set
        while not stop_flag.is_set():
            # Poll for new message, non-blocking in order for event flag to work
            msg = self.consumer.poll(timeout= self.config.KAKFA_POLL_TIMEOUT)
            # If a message was caught, process it
            if msg is not None:
                # Broad try except for now to skip faulty data
                try:
                    post = HackerNewsPost(**json.loads(msg.value()))
                    logger.debug(f"Recieved insert request with id: {post.id}")
                    # Embed the post into a list of MilvusDocs, each containing a chunk
                    res = self.embed_post(post)
                    # Produce the result to the insert channel
                    self.respond_insert(res)
                except Exception as e:
                    logger.error(f"Failed to embed post: {post.id}, {e}")
                # self.consumer.commit(msg)
                    
        # Flush producer on finish
        self.producer.flush()
        logger.debug("Exiting InsertEmbedder run() loop")
        return

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
                if doc.page_content is not None:
                    embedding = self.embedder.embed_query(doc.page_content)
                    new_doc = MilvusDocument(
                        chunk_id=str(post.id)
                        + "_"
                        + str(i),  # Chunk id is the original id + chunk number
                        doc_id=str(
                            post.id
                        ),  # Convert the original id to string for Milvus
                        chunk=doc.page_content,  # Chunk is the original text of the chunk
                        title=post.title,
                        by=post.by,
                        url=post.url,
                        embedding=embedding,
                    )
                    milvus_docs.append(new_doc)

        # If there is no URL, its a text post, requiring only the text inside to be embedded
        elif post.text is not None:
            # Split the text of the post and embed each
            texts = self.text_splitter.split_text(post.text)
            for i, text in enumerate(texts):
                embedding = self.embedder.embed_query(text)
                new_doc = MilvusDocument(
                    chunk_id=str(post.id) + "_" + str(i),
                    doc_id=str(post.id),
                    chunk=text,
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
                topic= self.config.KAFKA_TOPICS["INSERT_REQUEST_TOPIC"],
                value=json.dumps(x.model_dump(exclude_none=True)),
            )
            # self.producer.flush()
            logger.debug(f"Insert for document: {x.doc_id} sent to insert topic")
