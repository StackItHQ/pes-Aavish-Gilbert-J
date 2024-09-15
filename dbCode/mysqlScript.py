import mysql.connector
import json

def sync_sheet_from_json(db, json_input):
    cursor = db.cursor()
    try:
        # Start a transaction
        db.start_transaction()
        print("Transaction started.")

        # Parse the JSON input
        data = json.loads(json_input) if isinstance(json_input, str) else json_input
        print("JSON parsed successfully:", data)

        # Get or create the sheet
        sheet_name = data["sheet_name"]
        print(f"Sheet name: {sheet_name}")
        sheet_id = get_or_create_sheet(db, cursor, sheet_name)
        print(f"Sheet ID: {sheet_id}")

        # Process each cell in the input
        for cell in data.get("cells", []):
            row_num = cell.get("row")
            col_num = cell.get("column")
            value = cell.get("value")  # Value can be None for deletion
            operation = cell.get("operation", "upsert")
            print(f"Processing cell - Row: {row_num}, Column: {col_num}, Value: {value}, Operation: {operation}")

            if operation == "delete":
                if row_num is not None and col_num is not None:
                    print(f"Deleting cell at Row: {row_num}, Column: {col_num}")
                    delete_cell(cursor, sheet_id, row_num, col_num)
                elif row_num is not None:
                    print(f"Deleting row: {row_num}")
                    delete_row_or_column(cursor, sheet_id, row_num=row_num)
                elif col_num is not None:
                    print(f"Deleting column: {col_num}")
                    delete_row_or_column(cursor, sheet_id, col_num=col_num)
                else:
                    print("No valid row or column specified for deletion.")
            else:
                print(f"Upserting cell at Row: {row_num}, Column: {col_num}, Value: {value}")
                upsert_cell(cursor, sheet_id, row_num, col_num, value)

        # Commit the transaction
        db.commit()
        print("Transaction committed.")

    except mysql.connector.Error as err:
        # Rollback transaction in case of any error
        db.rollback()
        print(f"Database Error during sync: {err}")
    except json.JSONDecodeError as json_err:
        print(f"JSON parsing error: {json_err}")
    except Exception as e:
        # Handle any other exceptions
        db.rollback()
        print(f"Unexpected error during sync: {e}")
    finally:
        cursor.close()
        print("Cursor closed.")

def get_or_create_sheet(db, cursor, sheet_name):
    try:
        print(f"Fetching sheet with name: {sheet_name}")
        cursor.execute("SELECT id FROM sheets WHERE name = %s", (sheet_name,))
        sheet = cursor.fetchone()

        if sheet is None:
            print(f"Sheet not found, creating new sheet: {sheet_name}")
            cursor.execute("INSERT INTO sheets (name) VALUES (%s)", (sheet_name,))
            sheet_id = cursor.lastrowid
            print(f"New sheet created with ID: {sheet_id}")
            return sheet_id
        print(f"Sheet found with ID: {sheet[0]}")
        return sheet[0]
    except mysql.connector.Error as err:
        print(f"Error in get_or_create_sheet: {err}")
        raise
    except Exception as e:
        print(f"Unexpected error in get_or_create_sheet: {e}")
        raise

def upsert_cell(cursor, sheet_id, row_num, col_num, value):
    try:
        if row_num is None or col_num is None:
            print("Row number and column number must be provided for upsert operation.")
            return
        print(f"Processing upsert for cell at row {row_num}, column {col_num}")

        if value is None or value == "":
            print(f"Value is None, deleting cell at row {row_num}, column {col_num}")
            delete_cell(cursor, sheet_id, row_num, col_num)
            return

        # Check if the cell exists
        query = """
            SELECT id FROM cells 
            WHERE sheet_id = %s AND `row_number` = %s AND `column_number` = %s
        """
        print(f"Executing query to check if cell exists with sheet_id={sheet_id}, row_number={row_num}, column_number={col_num}")
        cursor.execute(query, (sheet_id, row_num, col_num))
        cell = cursor.fetchone()

        if cell:
            print(f"Cell exists with ID {cell[0]}, updating value.")
            update_query = """
                UPDATE cells 
                SET value = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """
            print(f"Executing update query with value: {value}, id: {cell[0]}")
            cursor.execute(update_query, (value, cell[0]))
            print(f"Updated cell at row {row_num}, col {col_num} with value: {value}")
        else:
            print(f"Cell does not exist, inserting new cell.")
            insert_query = """
                INSERT INTO cells (sheet_id, `row_number`, `column_number`, value)
                VALUES (%s, %s, %s, %s)
            """
            print(f"Executing insert query with sheet_id={sheet_id}, row_number={row_num}, column_number={col_num}, value={value}")
            cursor.execute(insert_query, (sheet_id, row_num, col_num, value))
            print(f"Inserted new cell at row {row_num}, col {col_num} with value: {value}")
    except mysql.connector.Error as err:
        print(f"Error in upsert_cell: {err}")
        raise
    except Exception as e:
        print(f"Unexpected error in upsert_cell: {e}")
        raise

def delete_cell(cursor, sheet_id, row_num, col_num):
    try:
        print(f"Deleting cell at row {row_num}, column {col_num} from sheet {sheet_id}")
        delete_query = """
            DELETE FROM cells 
            WHERE sheet_id = %s AND `row_number` = %s AND `column_number` = %s
        """
        cursor.execute(delete_query, (sheet_id, row_num, col_num))
        print(f"Deleted cell at row {row_num}, column {col_num} from sheet {sheet_id}")
    except mysql.connector.Error as err:
        print(f"Error in delete_cell: {err}")
        raise
    except Exception as e:
        print(f"Unexpected error in delete_cell: {e}")
        raise

def delete_row_or_column(cursor, sheet_id, row_num=None, col_num=None):
    try:
        if row_num is not None:
            print(f"Deleting row {row_num} from sheet {sheet_id}")
            delete_query = "DELETE FROM cells WHERE sheet_id = %s AND `row_number` = %s"
            cursor.execute(delete_query, (sheet_id, row_num))
            print(f"Deleted row {row_num} from sheet {sheet_id}")

        if col_num is not None:
            print(f"Deleting column {col_num} from sheet {sheet_id}")
            delete_query = "DELETE FROM cells WHERE sheet_id = %s AND `column_number` = %s"
            cursor.execute(delete_query, (sheet_id, col_num))
            print(f"Deleted column {col_num} from sheet {sheet_id}")

    except mysql.connector.Error as err:
        print(f"Error in delete_row_or_column: {err}")
        raise
    except Exception as e:
        print(f"Unexpected error in delete_row_or_column: {e}")
        raise

# Example usage
if __name__ == "__main__":
    # Example JSON input
    json_input = '''
    {
        "sheet_name": "Sheet2",
        "cells": [
            {"row": 3, "column": 3, "value": "bossman", "operation": "insert"},
            {"row": 3, "column": 3, "operation": "delete"},
            {"row": 2, "operation": "delete"},
            {"column": 2, "operation": "delete"}
        ]
    }
    '''

    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="your_mysql_password",  # Replace with your MySQL root password
        database="google_sheet_mimic"
    )

    print("Starting sync process...")
    sync_sheet_from_json(db, json_input)
    print("Sync process complete.")
    db.close()
