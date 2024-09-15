from flask import Flask, request, jsonify
import mysql.connector
import psycopg2
from kafka import KafkaProducer, KafkaConsumer
import json

from dbCode.mysqlScript import sync_sheet_from_json

app = Flask(__name__)

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'sync_topic'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# MySQL Database Configuration
mysql_db = mysql.connector.connect(
    host="localhost",        # MySQL server host
    user="root",             # MySQL username (in this case, 'root')
    password="Aavish@02", # MySQL root password
    database="superjoin"  # Database name
)

# PostgreSQL Database Configuration
postgres_db = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="Aavish@02",
    database="superjoin"
)


@app.route('/mysql_update', methods=['POST'])
def mysql_update():
    """
    Endpoint to update MySQL database, and if the record doesn't exist, create it dynamically.
    """
    try:
        data = request.json
        sync_sheet_from_json(mysql_db, data)

        # Send the update to Kafka
        # sendToKafka(data)

        return jsonify({"status": "success", "message": "MySQL database updated"}), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500



@app.route('/postgres_update', methods=['POST'])
def postgres_update():
    """
    Endpoint to update PostgreSQL database.
    """
    try:
        data = request.json
        cursor = postgres_db.cursor()
        query = "UPDATE your_table SET column_name = %s WHERE id = %s"
        cursor.execute(query, (data['value'], data['id']))
        postgres_db.commit()

        # Send the update to Kafka
        sendToKafka(data)

        return jsonify({"status": "success", "message": "PostgreSQL database updated"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/sync_google_sheet', methods=['POST'])
def sync_google_sheet():
    """
    Endpoint to synchronize data from Google Sheets to the databases.
    """
    try:
        data = request.json
        table = data.get("table")
        row_data = data.get("data")

        # Sync data to MySQL
        cursor_mysql = mysql_db.cursor()
        query_mysql = "INSERT INTO {} (columns) VALUES (%s, %s, ...)".format(table)
        cursor_mysql.execute(query_mysql, tuple(row_data))
        mysql_db.commit()

        # Sync data to PostgreSQL
        cursor_postgres = postgres_db.cursor()
        query_postgres = "INSERT INTO {} (columns) VALUES (%s, %s, ...)".format(table)
        cursor_postgres.execute(query_postgres, tuple(row_data))
        postgres_db.commit()

        # Send the sync data to Kafka
        sendToKafka(data)

        return jsonify({"status": "success", "message": "Google Sheets data synchronized"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/conflict_resolution', methods=['POST'])
def conflict_resolution():
    """
    Endpoint to resolve data conflicts between systems.
    """
    try:
        data = request.json
        resolved_data = handleConflicts(data)
        return jsonify({"status": "success", "resolved_data": resolved_data}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


def sendToKafka(data):
    """
    Sends data to a Kafka topic.
    """
    try:
        producer.send(KAFKA_TOPIC, data)
        producer.flush()
        print("Data sent to Kafka successfully")
    except Exception as e:
        print(f"Error sending data to Kafka: {e}")


def readFromKafka():
    """
    Reads data from a Kafka topic.
    """
    try:
        for message in consumer:
            print(f"Consumed message from Kafka: {message.value}")
            handleConflicts(message.value)
    except Exception as e:
        print(f"Error consuming Kafka message: {e}")


def handleConflicts(data):
    """
    Handles conflicts in the data.
    """
    # Example conflict resolution logic
    if data.get("conflict"):
        # Resolve the conflict by modifying the data
        data["resolved"] = True
        data["new_value"] = data.get("value", 0) * 2  # Custom conflict resolution logic
        print("Conflict resolved:", data)
        return data
    else:
        print("No conflict detected.")
        return data


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
