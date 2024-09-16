import mysql.connector
import requests
import time

# Configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'sheet_sync_user',
    'password': 'Aavish@02',
    'database': 'google_sheet_mimic'
}

APP_SCRIPT_URL = 'https://script.google.com/macros/s/AKfycbx0Ffi116BJDJPlH6bLHKJAGb8yNnU-By2Fp0kLySJ1ruJt-jV7nhlpyAwWGGKKwW7UCg/exec' 

def fetch_unprocessed_changes(db_conn):
    cursor = db_conn.cursor(dictionary=True)
    query = "SELECT * FROM cell_changes WHERE processed = 0 ORDER BY changed_at ASC"
    cursor.execute(query)
    changes = cursor.fetchall()
    cursor.close()
    return changes

def mark_changes_as_processed(db_conn, change_ids):
    cursor = db_conn.cursor()
    format_strings = ','.join(['%s'] * len(change_ids))
    query = "UPDATE cell_changes SET processed = 1 WHERE id IN (%s)" % format_strings
    cursor.execute(query, tuple(change_ids))
    db_conn.commit()
    cursor.close()

def logUpdate():
    db_conn = mysql.connector.connect(**DB_CONFIG)

    try:
        while True:
            changes = fetch_unprocessed_changes(db_conn)
            if changes:
                # Send changes to Apps Script
                payload = {'changes': changes}
                response = requests.post(APP_SCRIPT_URL, json=payload)

                if response.status_code == 200:
                    print("Changes sent successfully.")
                    # Mark changes as processed
                    change_ids = [change['id'] for change in changes]
                    mark_changes_as_processed(db_conn, change_ids)
                else:
                    print(f"Failed to send changes. Status code: {response.status_code}")
                    print(f"Response: {response.text}")
            else:
                print("No new changes.")

            # Wait before checking again
            time.sleep(2)  # Adjust the interval as needed

    except KeyboardInterrupt:
        print("Stopping the monitoring script.")
    finally:
        db_conn.close()

if __name__ == "__main__":
    logUpdate()
