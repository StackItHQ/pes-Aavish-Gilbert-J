
# from flask import Flask, request, jsonify
# import logging
# import threading
# import mysql.connector
# import psycopg2
# from twoWayKafka import KafkaHandler
# from dbCode.mysqlScript import sync_sheet_from_json
# from updateSheet import logUpdate

# logging.basicConfig(level=logging.INFO)


# # --- Database Configurations ---

# # MySQL Database Configuration
# mysql_db = mysql.connector.connect(
#     host="localhost",        # MySQL server host
#     user="root",             # MySQL username
#     password="Aavish@02",    # MySQL password
#     database="superjoin"  # Database name
# )




# # --- Kafka Consumer Thread Function ---

# def run_consumer(kafka_handler):
#     def process_message(msg):
#         # Process the message
#         sync_sheet_from_json(mysql_db, msg)
        
#         # Prepare response
#         correlation_id = msg.get('correlation_id')
#         if correlation_id:
#             response = {
#                 'status': 'success',
#                 'correlation_id': correlation_id,
#                 'result': 'Data synchronized'
#             }
#             # Send response back to the response_topic
#             kafka_handler.producer.send(kafka_handler.response_topic, response)
#             kafka_handler.producer.flush()
    
#     kafka_handler.consume_messages(process_message)

    

# # --- Flask Application ---

# app = Flask(__name__)

# # Create a KafkaHandler instance with response_topic for two-way communication
# kafka_handler = KafkaHandler(
#     bootstrap_servers='localhost:9092',
#     topic='my_topic',
#     group_id='my_group',
#     response_topic='response_topic'  # Replace with your actual response topic
# )

# # Start Kafka consumer in a background thread
# consumer_thread = threading.Thread(target=run_consumer, args=(kafka_handler,), daemon=True)
# consumer_thread.start()

# # db_trigger = threading.Thread(target=logUpdate, daemon=True)
# # db_trigger.start()


# # Flask route to publish message to Kafka and wait for a response
# @app.route('/kafka-publish-endpoint', methods=['POST'])
# def kafka_publish():
#     """
#     Endpoint to publish a message to Kafka and wait for a response.
#     Expects a JSON payload in the request.
#     """
#     data = request.json
#     if not data:
#         return jsonify({"status": "error", "message": "Invalid payload"}), 400

#     # Send a request and wait for a response
#     response = kafka_handler.send_request(data, timeout=10)
#     return jsonify(response)

# # Start Flask server
# if __name__ == '__main__':
#     app.run(host='0.0.0.0', port=5000)



#==========================================================================================================================


# from flask import Flask, request, jsonify
# import logging
# import threading
# import mysql.connector
# import json
# import time
# from twoWayKafka import KafkaHandler
# from dbCode.mysqlScript import sync_sheet_from_json
# # Uncomment the following line if you need to use logUpdate
# # from updateSheet import logUpdate

# logging.basicConfig(level=logging.INFO)

# # --- Database Configurations ---

# # MySQL Database Configuration
# db_config = {
#     'host': 'localhost',        # MySQL server host
#     'user': 'root',             # MySQL username
#     'password': 'Aavish@02',    # MySQL password
#     'database': 'superjoin'     # Database name
# }

# # --- Kafka Configuration ---

# kafka_config = {
#     'bootstrap_servers': 'localhost:9092',
#     'topic': 'my_topic',
#     'group_id': 'my_group',
#     'response_topic': 'response_topic'  # Replace with your actual response topic
# }

# # --- Flask Application ---

# app = Flask(__name__)

# # Create a KafkaHandler instance with response_topic for two-way communication
# kafka_handler = KafkaHandler(
#     bootstrap_servers=kafka_config['bootstrap_servers'],
#     topic=kafka_config['topic'],
#     group_id=kafka_config['group_id'],
#     response_topic=kafka_config['response_topic']
# )

# # --- Function to Read and Write Last Processed ID ---

# def read_last_id():
#     try:
#         with open('last_id.txt', 'r') as f:
#             return int(f.read())
#     except Exception:
#         return 0

# def write_last_id(last_id):
#     with open('last_id.txt', 'w') as f:
#         f.write(str(last_id))

# # --- Cells Monitoring Function ---

# def monitor_cells(kafka_handler, db_config):
#     last_id = read_last_id()  # Read from file or initialize to 0
#     while True:
#         try:
#             connection = mysql.connector.connect(**db_config)
#             cursor = connection.cursor(dictionary=True)
#             query = """
#                 SELECT * FROM cells
#                 WHERE changed_by = 'sheet_sync_user@localhost'
#                   AND operation = 'insert'
#                   AND id > %s
#                 ORDER BY id ASC
#             """
#             cursor.execute(query, (last_id,))
#             changes = cursor.fetchall()
#             cursor.close()
#             connection.close()
#             if changes:
#                 for change in changes:
#                     # Log the addition
#                     logging.info(f"Cell added by 'sheet_sync_user': {change}")

#                     message = {
#                         'id': change['id'],
#                         'sheet_id': change['sheet_id'],
#                         'row_number': change['row_number'],
#                         'column_number': change['column_number'],
#                         'value': change['value'],
#                         'operation': change['operation'],
#                         'changed_by': change['changed_by'],
#                         'changed_at': change['changed_at'].strftime('%Y-%m-%d %H:%M:%S'),
#                         'is_current': change['is_current']
#                     }
#                     # Publish message to Kafka using KafkaHandler
#                     kafka_handler.publish_message(message)
#                     logging.info(f"Sent message: {message}")

#                 last_id = changes[-1]['id']
#                 write_last_id(last_id)  # Save the last processed ID
#             else:
#                 time.sleep(1)
#         except Exception as e:
#             logging.error(f"Error in monitor_cells: {e}")
#             time.sleep(1)

# # Start the monitor_cells function in a background thread
# monitor_thread = threading.Thread(target=monitor_cells, args=(kafka_handler, db_config), daemon=True)
# monitor_thread.start()

# # --- Kafka Consumer Thread Function ---

# def run_consumer(kafka_handler):
#     def process_message(msg):
#         # Process the message
#         sync_sheet_from_json(mysql.connector.connect(**db_config), msg)
        
#         # Prepare response
#         correlation_id = msg.get('correlation_id')
#         if correlation_id:
#             response = {
#                 'status': 'success',
#                 'correlation_id': correlation_id,
#                 'result': 'Data synchronized'
#             }
#             # Send response back to the response_topic
#             kafka_handler.producer.send(kafka_handler.response_topic, response)
#             kafka_handler.producer.flush()
    
#     kafka_handler.consume_messages(process_message)

# # Start Kafka consumer in a background thread
# consumer_thread = threading.Thread(target=run_consumer, args=(kafka_handler,), daemon=True)
# consumer_thread.start()

# # Uncomment the following lines if you need to use logUpdate
# # db_trigger = threading.Thread(target=logUpdate, daemon=True)
# # db_trigger.start()

# # --- Flask Route to Publish Message to Kafka and Wait for a Response ---

# @app.route('/kafka-publish-endpoint', methods=['POST'])
# def kafka_publish():
#     """
#     Endpoint to publish a message to Kafka and wait for a response.
#     Expects a JSON payload in the request.
#     """
#     data = request.json
#     if not data:
#         return jsonify({"status": "error", "message": "Invalid payload"}), 400

#     # Send a request and wait for a response
#     response = kafka_handler.send_request(data, timeout=10)
#     return jsonify(response)

# # Start Flask server
# if __name__ == '__main__':
#     app.run(host='0.0.0.0', port=5000)


from flask import Flask, request, jsonify
import logging
import threading
import mysql.connector
import json
import time
from twoWayKafka import KafkaHandler
from dbCode.mysqlScript import sync_sheet_from_json

logging.basicConfig(level=logging.INFO)

# --- Database Configurations ---
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Aavish@02',
    'database': 'superjoin'
}

# --- Kafka Configuration ---
kafka_config = {
    'bootstrap_servers': 'localhost:9092',
    'topic': 'my_topic',           # For A -> B communication
    'group_id': 'my_group',
    'response_topic': 'response_topic',
    'cells_topic': 'cells_topic',  # New topic for C -> D communication
    'cells_group_id': 'cells_group'
}

# --- Flask Application ---
app = Flask(__name__)

# Create KafkaHandler instances for both A -> B and C -> D communications
kafka_handler = KafkaHandler(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    topic=kafka_config['topic'],
    group_id=kafka_config['group_id'],
    response_topic=kafka_config['response_topic']
)

kafka_handler_cells = KafkaHandler(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    topic=kafka_config['cells_topic'],
    group_id=kafka_config['cells_group_id']
)

# --- Function to Read and Write Last Processed ID ---
def read_last_id():
    try:
        with open('last_id.txt', 'r') as f:
            return int(f.read())
    except Exception:
        return 0

def write_last_id(last_id):
    with open('last_id.txt', 'w') as f:
        f.write(str(last_id))

# --- Cells Monitoring Function (C) ---
def monitor_cells(kafka_handler_cells, db_config):
    last_id = read_last_id()  # Read from file or initialize to 0
    logging.info(f"Starting monitor_cells with last_id: {last_id}")

    while True:
        try:
            # Step 1: Connect to the MySQL database
            logging.debug("Connecting to the database...")
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor(dictionary=True)

            # Step 2: Prepare and execute the query
            query = """
                SELECT * FROM cells
                WHERE changed_by = 'sheet_sync_user@localhost'
                  AND operation = 'insert'
                  AND id > %s
                ORDER BY id ASC
            """
            logging.debug(f"Executing query with last_id: {last_id}")
            cursor.execute(query, (last_id,))
            changes = cursor.fetchall()

            logging.debug(f"Query executed. Number of changes fetched: {len(changes)}")
            cursor.close()
            connection.close()

            # Step 3: Process the changes
            if changes:
                for change in changes:
                    # Debug each change being processed
                    logging.debug(f"Processing change: {change}")

                    # Prepare the message to send to Kafka
                    message = {
                        'id': change['id'],
                        'sheet_id': change['sheet_id'],
                        'row_number': change['row_number'],
                        'column_number': change['column_number'],
                        'value': change['value'],
                        'operation': change['operation'],
                        'changed_by': change['changed_by'],
                        'changed_at': change['changed_at'].strftime('%Y-%m-%d %H:%M:%S'),
                        'is_current': change['is_current']
                    }
                    # Log the message being sent to Kafka
                    logging.info(f"Publishing message to Kafka: {message}")
                    kafka_handler_cells.publish_message(message)
                    logging.debug(f"Message sent successfully: {message}")

                # Update the last processed ID and store it
                last_id = changes[-1]['id']
                logging.info(f"Updating last_id to: {last_id}")
                write_last_id(last_id)
            else:
                # Log when no changes are found
                logging.debug("No changes found. Sleeping for 1 second...")
                time.sleep(1)

        except mysql.connector.Error as db_error:
            # Log database connection or query errors
            logging.error(f"Database error in monitor_cells: {db_error}")
            time.sleep(1)
        except Exception as e:
            # Catch all other errors
            logging.error(f"Unexpected error in monitor_cells: {e}")
            time.sleep(1)


# Start the monitor_cells function in a background thread
monitor_thread = threading.Thread(target=monitor_cells, args=(kafka_handler_cells, db_config), daemon=True)
monitor_thread.start()

# --- Kafka Consumer for C -> D (Listening on cells_topic) ---
def run_cells_consumer(kafka_handler_cells):
    """
    Consumes messages from the Kafka cells_topic and processes them.
    Acts as the D part of C -> D communication.
    """
    def process_cells_message(msg):
    # Process the message (C -> D flow)
        logging.info(f"Processing message from cells_topic: {msg}")

        # Simulate processing the request and send a response
        correlation_id = msg.get('correlation_id')
        if correlation_id:
            response = {
                'status': 'success',
                'correlation_id': correlation_id,
                'result': 'Cells processed successfully'
            }
            # Send response back to the response_topic
            kafka_handler_cells.producer.send(kafka_handler_cells.response_topic, response)
            kafka_handler_cells.producer.flush()
            logging.info(f"Response sent for correlation_id: {correlation_id}")

    kafka_handler_cells.consume_messages(process_cells_message)

# Start Kafka consumer for C -> D in a background thread
cells_consumer_thread = threading.Thread(target=run_cells_consumer, args=(kafka_handler_cells,), daemon=True)
cells_consumer_thread.start()

# --- Kafka Consumer Thread Function for A -> B (Kafka sync) ---
def run_consumer(kafka_handler):
    def process_message(msg):
        # Process the message
        sync_sheet_from_json(mysql.connector.connect(**db_config), msg)
        
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

# Start Kafka consumer for A -> B in a background thread
consumer_thread = threading.Thread(target=run_consumer, args=(kafka_handler,), daemon=True)
consumer_thread.start()

# --- Flask Route to Publish Message to Kafka and Wait for a Response (A -> B) ---
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
