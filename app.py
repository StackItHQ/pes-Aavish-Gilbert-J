# from flask import Flask, request, jsonify
# from kafka import KafkaProducer, KafkaConsumer
# from kafka.errors import KafkaError
# import json
# import logging
# import threading
# import mysql.connector
# import psycopg2
# from dbCode.mysqlScript import sync_sheet_from_json
# logging.basicConfig(level=logging.INFO)


# # MySQL Database Configuration
# mysql_db = mysql.connector.connect(
#     host="localhost",        # MySQL server host
#     user="root",             # MySQL username (in this case, 'root')
#     password="Aavish@02", # MySQL root password
#     database="google_sheet_mimic"  # Database name
# )

# # PostgreSQL Database Configuration
# postgres_db = psycopg2.connect(
#     host="localhost",
#     user="postgres",
#     password="Aavish@02",
#     database="superjoin"
# )


# class KafkaHandler:
#     def __init__(self, bootstrap_servers, topic, group_id=None):
#         self.bootstrap_servers = bootstrap_servers
#         self.topic = topic
#         self.group_id = group_id

#         self.producer = KafkaProducer(
#             bootstrap_servers=self.bootstrap_servers,
#             value_serializer=lambda v: json.dumps(v).encode('utf-8')
#         )
#         # Remove KafkaConsumer initialization from here

#     def publish_message(self, message):
#         """
#         Publishes a message to the Kafka topic.
#         """
#         try:
#             future = self.producer.send(self.topic, message)
#             result = future.get(timeout=10)
#             logging.info(f"Message sent successfully: {result}")
#             return {"status": "success", "result": str(result)}
#         except KafkaError as e:
#             logging.error(f"Failed to send message: {e}")
#             return {"status": "error", "message": str(e)}
        
#     def consume_messages(self, process_callback):
#         """
#         Initializes the consumer and consumes messages from the Kafka topic asynchronously.
#         Passes each message to the provided callback function (e.g., sync_sheet_from_json).
#         """
#         logging.info(f"Starting consumer for topic: {self.topic}")
#         try:
#             consumer = KafkaConsumer(
#                 self.topic,
#                 group_id=self.group_id,
#                 bootstrap_servers=self.bootstrap_servers,
#                 value_deserializer=lambda m: json.loads(m.decode('utf-8')),
#                 auto_offset_reset='earliest',  # Start from the earliest message if offset is not committed
#                 enable_auto_commit=True,  # Automatically commit message offset after processing
#             )
#             for message in consumer:
#                 process_callback(message.value)
#         except KafkaError as e:
#             logging.error(f"Error consuming messages: {e}")

# # Function to run Kafka consumer in a separate thread
# def run_consumer(kafka_handler):
#     kafka_handler.consume_messages(lambda msg: sync_sheet_from_json(mysql_db, msg))

# # Create Flask App
# app = Flask(__name__)

# # Create a KafkaHandler instance
# kafka_handler = KafkaHandler(bootstrap_servers='localhost:9092', topic='my_topic', group_id='my_group')

# # Start Kafka consumer in a background thread
# consumer_thread = threading.Thread(target=run_consumer, args=(kafka_handler,), daemon=True)
# consumer_thread.start()

# # Flask route to publish message to Kafka
# @app.route('/kafka-publish-endpoint', methods=['POST'])
# def kafka_publish():
#     """
#     Endpoint to publish a message to Kafka.
#     Expects a JSON payload in the request.
#     """
#     data = request.json
#     if not data:
#         return jsonify({"status": "error", "message": "Invalid payload"}), 400

#     response = kafka_handler.publish_message(data)
#     return jsonify(response)

# # Start Flask server
# if __name__ == '__main__':
#     app.run(host='0.0.0.0', port=5000)

from flask import Flask, request, jsonify
import json
import logging
import threading
import uuid
import time
import mysql.connector
import psycopg2
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from dbCode.mysqlScript import sync_sheet_from_json

logging.basicConfig(level=logging.INFO)

# --- KafkaHandler Class with Two-Way Message Passing ---

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

# --- Database Configurations ---

# MySQL Database Configuration
mysql_db = mysql.connector.connect(
    host="localhost",        # MySQL server host
    user="root",             # MySQL username
    password="Aavish@02",    # MySQL password
    database="google_sheet_mimic"  # Database name
)

# PostgreSQL Database Configuration
postgres_db = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="Aavish@02",
    database="superjoin"
)

# --- Kafka Consumer Thread Function ---

def run_consumer(kafka_handler):
    def process_message(msg):
        # Process the message
        sync_sheet_from_json(mysql_db, msg)
        
        # Prepare response
        correlation_id = msg.get('correlation_id')
        if correlation_id:
            response = {
                'status': 'success',
                'correlation_id': correlation_id,
                'result': 'Data synchronized'
            }
            # Send response back to the response_topic
            kafka_handler.producer.send(kafka_handler.response_topic, response)
            kafka_handler.producer.flush()
    
    kafka_handler.consume_messages(process_message)

    

# --- Flask Application ---

app = Flask(__name__)

# Create a KafkaHandler instance with response_topic for two-way communication
kafka_handler = KafkaHandler(
    bootstrap_servers='localhost:9092',
    topic='my_topic',
    group_id='my_group',
    response_topic='response_topic'  # Replace with your actual response topic
)

# Start Kafka consumer in a background thread
consumer_thread = threading.Thread(target=run_consumer, args=(kafka_handler,), daemon=True)
consumer_thread.start()

# Flask route to publish message to Kafka and wait for a response
@app.route('/kafka-publish-endpoint', methods=['POST'])
def kafka_publish():
    """
    Endpoint to publish a message to Kafka and wait for a response.
    Expects a JSON payload in the request.
    """
    data = request.json
    if not data:
        return jsonify({"status": "error", "message": "Invalid payload"}), 400

    # Send a request and wait for a response
    response = kafka_handler.send_request(data, timeout=10)
    return jsonify(response)

# Start Flask server
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
