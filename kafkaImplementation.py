from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import json
import logging
from concurrent.futures import ThreadPoolExecutor
import signal
import sys

logging.basicConfig(level=logging.INFO)

class KafkaHandler:
    def __init__(self, bootstrap_servers, topic, group_id=None):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.executor = ThreadPoolExecutor(max_workers=10)  # Use a thread pool for async message consumption

        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        self.consumer = KafkaConsumer(
            self.topic,
            group_id=self.group_id,
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',  # Start from the earliest message if offset is not committed
            enable_auto_commit=True,  # Automatically commit message offset after processing
        )
        
        # Graceful shutdown on SIGINT (Ctrl+C)
        signal.signal(signal.SIGINT, self.shutdown)

    def publish_message(self, message):
        """
        Publishes a message to the Kafka topic.
        """
        try:
            future = self.producer.send(self.topic, message)
            result = future.get(timeout=10)
            logging.info(f"Message sent successfully: {result}")
        except KafkaError as e:
            logging.error(f"Failed to send message: {e}")
    
    def consume_messages(self):
        """
        Consumes messages from the Kafka topic asynchronously.
        """
        logging.info(f"Consuming messages from topic: {self.topic}")
        try:
            for message in self.consumer:
                # Submit message processing to the thread pool to avoid blocking the main thread
                self.executor.submit(self.process_message, message.value)
        except KafkaError as e:
            logging.error(f"Error consuming messages: {e}")

    def process_message(self, message):
        """
        Processes the Kafka message and handles conflicts if necessary.
        """
        logging.info(f"Processing message: {message}")
        self.handle_conflicts(message)

    def handle_conflicts(self, message):
        """
        Handles conflicts based on message content. Custom conflict resolution logic can be added here.
        """
        # Example conflict resolution logic (you can expand this logic based on your requirements)
        if "conflict" in message:
            logging.warning("Conflict detected, handling conflict...")
            resolved_message = self.resolve_conflict(message)
            logging.info(f"Resolved message: {resolved_message}")
            self.publish_message(resolved_message)
        else:
            logging.info("No conflict detected.")

    def resolve_conflict(self, message):
        """
        Resolves conflicts in the message (example logic).
        """
        # Example resolution: append '_resolved' to the conflicting data
        message['data'] = message.get('data', '') + '_resolved'
        return message

    def shutdown(self, signum, frame):
        """
        Gracefully shutdown consumer and producer.
        """
        logging.info("Shutting down Kafka producer and consumer...")
        self.consumer.close()
        self.producer.close()
        self.executor.shutdown(wait=False)
        sys.exit(0)

# Usage
kafka = KafkaHandler(bootstrap_servers='localhost:9092', topic='my_topic', group_id='my_group')
kafka.consume_messages()
