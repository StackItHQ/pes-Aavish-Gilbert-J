import mysql.connector
import json

def sync_sheet_from_json(db, json_input):
    try:
        # Start a transaction
        cursor = db.cursor()
        
        # Parse the JSON
        data = json.loads(json_input)
        
        # Get or create the sheet
        sheet_name = data["sheet_name"]
        sheet_id = get_or_create_sheet(db, sheet_name)
        
        # Process each cell in the input
        for cell in data["cells"]:
            row_num = cell["row"]
            col_num = cell["column"]
            value = cell.get("value")  # Value can be None for deletion

            if cell.get("operation") == "delete":
                if "row" in cell:
                    delete_row_or_column(db, sheet_id, row_num=row_num)
                if "column" in cell:
                    delete_row_or_column(db, sheet_id, col_num=col_num)
            else:
                # Handle insert or update
                upsert_cell(db, sheet_id, row_num, col_num, value)
        
        # Commit the transaction
        db.commit()

    except mysql.connector.Error as err:
        # Rollback transaction in case of any error
        db.rollback()
        return f"Error: {err}"
    except json.JSONDecodeError as json_err:
        print(f"JSON parsing error: {json_err}")
    except Exception as e:
        # Handle any other exceptions
        db.rollback()
        return f"Unexpected error: {e}"
    finally:
        cursor.close()

# Function to find or create a sheet with exception handling
def get_or_create_sheet(db, sheet_name):
    try:
        cursor = db.cursor()
        cursor.execute("SELECT id FROM sheets WHERE name = %s", (sheet_name,))
        sheet = cursor.fetchone()

        if sheet is None:
            cursor.execute("INSERT INTO sheets (name) VALUES (%s)", (sheet_name,))
            db.commit()
            return cursor.lastrowid
        return sheet[0]
    except mysql.connector.Error as err:
        db.rollback()
        return f"Error in get_or_create_sheet: {err}"
    finally:
        cursor.close()

# Function to update or insert cell data with exception handling
def upsert_cell(db, sheet_id, row_num, col_num, value):
    try:
        cursor = db.cursor()

        # Check if the cell exists
        cursor.execute("""
            SELECT id FROM cells 
            WHERE sheet_id = %s AND row_number = %s AND column_number = %s
        """, (sheet_id, row_num, col_num))
        cell = cursor.fetchone()

        if cell:
            # Update the existing cell value
            if value is not None:
                cursor.execute("""
                    UPDATE cells 
                    SET value = %s 
                    WHERE id = %s
                """, (value, cell[0]))
            else:
                # Set the value to NULL (for delete operation)
                cursor.execute("""
                    UPDATE cells 
                    SET value = NULL 
                    WHERE id = %s
                """, (cell[0],))
        else:
            # Insert new cell data
            cursor.execute("""
                INSERT INTO cells (sheet_id, row_number, column_number, value)
                VALUES (%s, %s, %s, %s)
            """, (sheet_id, row_num, col_num, value))
        
        db.commit()
    except mysql.connector.Error as err:
        db.rollback()
        return f"Error in upsert_cell: {err}"
    finally:
        cursor.close()

# Function to handle deleting a row or column with exception handling
def delete_row_or_column(db, sheet_id, row_num=None, col_num=None):
    try:
        cursor = db.cursor()

        if row_num is not None:
            cursor.execute("DELETE FROM cells WHERE sheet_id = %s AND row_number = %s", (sheet_id, row_num))
        if col_num is not None:
            cursor.execute("DELETE FROM cells WHERE sheet_id = %s AND column_number = %s", (sheet_id, col_num))
        
        db.commit()
    except mysql.connector.Error as err:
        db.rollback()
        return f"Error in delete_row_or_column: {err}"
    finally:
        cursor.close()



# # Example JSON input
# json_input = '''
# {
#     "sheet_name": "Sheet1",
#     "cells": [
#         {"row": 1, "column": 1, "value": "Product A"},
#         {"row": 1, "column": 2, "value": "Description A"},
#         {"row": 2, "column": 1, "value": "Product B"},
#         {"row": 2, "column": 2, "value": null, "operation": "delete"},  # Delete value from this cell
#         {"row": 3, "column": 1, "operation": "delete"}  # Delete entire row
#     ]
# }
# '''

# Sync the data
# sync_sheet_from_json(db, json_input)


