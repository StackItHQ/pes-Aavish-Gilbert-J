from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import json

class Kafka:
    def __init__(self, bootstrap_servers, topic):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
    
    def publishMessage(self, message):
        """
        Publishes a message to the Kafka topic.
        """
        try:
            future = self.producer.send(self.topic, message)
            result = future.get(timeout=10)
            print(f"Message sent successfully: {result}")
        except KafkaError as e:
            print(f"Failed to send message: {e}")
    
    def consumeMessage(self):
        """
        Consumes messages from the Kafka topic.
        """
        print(f"Consuming messages from topic: {self.topic}")
        try:
            for message in self.consumer:
                print(f"Received message: {message.value}")
                self.handleConflicts(message.value)
        except KafkaError as e:
            print(f"Error consuming messages: {e}")

    def handleConflicts(self, message):
        """
        Handles conflicts based on message content. Custom conflict resolution logic can be added here.
        """
        # Example conflict resolution logic (you can expand this logic based on your requirements)
        if "conflict" in message:
            print("Conflict detected, handling conflict...")
            # Add conflict resolution logic here
            resolved_message = self.resolveConflict(message)
            print(f"Resolved message: {resolved_message}")
            # Publish resolved message or take further action
        else:
            print("No conflict detected.")
    
    def resolveConflict(self, message):
        """
        Resolves conflicts in the message (example logic).
        """
        # Example resolution: append '_resolved' to the conflicting data
        message['data'] = message.get('data', '') + '_resolved'
        return message
