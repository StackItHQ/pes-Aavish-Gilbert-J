import uuid
import time
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import json
import threading
import logging

class KafkaHandler:
    def __init__(self, bootstrap_servers, topic, group_id=None, response_topic=None):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.response_topic = response_topic

        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        # Dictionary to hold pending responses
        self.pending_responses = {}
        self.lock = threading.Lock()

        if self.response_topic:
            # Start a consumer thread to listen to responses
            threading.Thread(target=self._start_response_consumer, daemon=True).start()

    def publish_message(self, message):
        """
        Publishes a message to the Kafka topic.
        """
        try:
            future = self.producer.send(self.topic, message)
            result = future.get(timeout=10)
            logging.info(f"Message sent successfully: {result}")
            return {"status": "success", "result": str(result)}
        except KafkaError as e:
            logging.error(f"Failed to send message: {e}")
            return {"status": "error", "message": str(e)}
            
    def consume_messages(self, process_callback):
        """
        Consumes messages from the Kafka topic asynchronously.
        Passes each message to the provided callback function.
        """
        logging.info(f"Starting consumer for topic: {self.topic}")
        try:
            consumer = KafkaConsumer(
                self.topic,
                group_id=self.group_id,
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
            )
            for message in consumer:
                process_callback(message.value)
        except KafkaError as e:
            logging.error(f"Error consuming messages: {e}")

    def send_request(self, message, timeout=10):
        """
        Sends a message and waits for a response.
        """
        correlation_id = str(uuid.uuid4())
        message['correlation_id'] = correlation_id

        with self.lock:
            self.pending_responses[correlation_id] = None

        try:
            future = self.producer.send(self.topic, message)
            future.get(timeout=10)
            logging.info(f"Request sent with correlation_id: {correlation_id}")
        except KafkaError as e:
            logging.error(f"Failed to send request: {e}")
            with self.lock:
                del self.pending_responses[correlation_id]
            return {"status": "error", "message": str(e)}

        # Wait for the response
        start_time = time.time()
        while True:
            with self.lock:
                response = self.pending_responses.get(correlation_id)
            if response is not None:
                with self.lock:
                    del self.pending_responses[correlation_id]
                return response
            elif time.time() - start_time > timeout:
                with self.lock:
                    del self.pending_responses[correlation_id]
                logging.error(f"Response timed out for correlation_id: {correlation_id}")
                return {"status": "error", "message": "Response timed out"}
            else:
                time.sleep(0.1)

    def _start_response_consumer(self):
        """
        Starts a consumer to listen for responses.
        """
        logging.info(f"Starting response consumer for topic: {self.response_topic}")
        try:
            consumer = KafkaConsumer(
                self.response_topic,
                group_id=self.group_id + '_response' if self.group_id else None,
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
            )
            for message in consumer:
                response = message.value
                correlation_id = response.get('correlation_id')
                if correlation_id:
                    with self.lock:
                        self.pending_responses[correlation_id] = response
        except KafkaError as e:
            logging.error(f"Error consuming responses: {e}")