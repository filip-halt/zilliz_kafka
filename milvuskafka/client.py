from copy import deepcopy
import json
from typing import Union
import uuid
from milvuskafka.config import Configuration
from confluent_kafka import Consumer, Producer

from milvuskafka.datatypes import MilvusSearchResponse


class Client():
    def __init__(self, config: Configuration):
        self.config = config

        self.kafka_producer_config = config.KAFKA_BASE_CONFIGS
        self.kafka_consumer_config = deepcopy(config.KAFKA_BASE_CONFIGS)
        self.kafka_consumer_config.update(
            {
                "enable.auto.commit": False,
                "group.id": "Client_Consumers",
                "auto.offset.reset": "earliest",
            }
        )

        self.consumer = Consumer(self.kafka_consumer_config)
        self.consumer.subscribe([config.KAFKA_TOPICS["SEARCH_RESPONSE_TOPIC"]])

        self.producer = Producer(self.kafka_producer_config)

    def request_documents(self, prompt: str, top_k: int) -> dict:
        request = {
            "query_id": uuid.uuid4().hex,
            "text": prompt,
            "top_k": top_k,
        }

        self.producer.produce(
            topic=self.config.KAFKA_TOPICS["SEARCH_EMBEDDING_TOPIC"],
            value=json.dumps(request),
        )
        # self.producer.flush()
    
    def parse_response(self, augment: bool = True) -> Union[MilvusSearchResponse, str]:
        msg = self.consumer.poll()
        res = MilvusSearchResponse(**json.loads(msg.value()))
        if augment:
            if len(res.results) == 0:
                return "Search did not return any values as context."
            import openai
            openai.api_key = self.config.OPENAI_KEY
            prompt_question = f"The question is: \n{res.text}"
            prompt = ""
            for x in res.results:
                context = x.chunk
                context = context.replace("\n", "")
                doc_id = "https://news.ycombinator.com/item?id=" + x.doc_id
    
                context_id = f"Context ID is:\n{doc_id}\n\n"
                context_prompt = f"Context is:\n{context}\n\n" 
                if len(prompt) + len(prompt_question) + len(context_id) + len(context_prompt) >= 4097:
                    break
                else:
                    prompt += context_id
                    prompt += context_prompt
    
            prompt += f"The question is: \n{res.text}"
            
            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a question answering bot that answers questions based soley on provided context. In your response you must reference each context by its ID in the answer."},
                    {"role": "user", "content": prompt}
                ]
            )
            res = response['choices'][0]['message']['content']
    
        self.consumer.commit(msg)

        return res