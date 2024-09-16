function onEditTrigger(e) {
  Logger.log("Triggered onEdit");

  // Check if the edited range contains data
  if (!e || !e.range) {
    Logger.log("No range or event detected");
    return;
  }

  // Get the sheet and range information
  var sheet = e.range.getSheet();
  var range = e.range;
  var sheetName = sheet.getName();
  var rowStart = range.getRow();
  var colStart = range.getColumn();
  var numRows = range.getNumRows();
  var numCols = range.getNumColumns();

  Logger.log("Sheet Name: " + sheetName);
  Logger.log("Row Start: " + rowStart + ", Column Start: " + colStart);
  Logger.log("Number of Rows: " + numRows + ", Number of Columns: " + numCols);

  var modifiedCells = [];

  // Iterate over all modified cells
  for (var i = 0; i < numRows; i++) {
    for (var j = 0; j < numCols; j++) {
      var cellRow = rowStart + i;
      var cellCol = colStart + j;
      var editedValue = sheet.getRange(cellRow, cellCol).getValue();
      var oldValue = e.oldValue; // Get the old value before the edit

      Logger.log("Processing cell: (" + cellRow + ", " + cellCol + ")");
      Logger.log("Old Value: " + oldValue + ", New Value: " + editedValue);

      // Determine the type of modification (inserted, updated, deleted)
      var action = '';
      if (!oldValue && editedValue) {
        action = 'insert';
        Logger.log("Action: insert");
      } else if (oldValue && !editedValue) {
        action = 'delete'; // Set action to delete if the cell is cleared
        Logger.log("Action: delete");
      } else if (oldValue !== editedValue) {
        action = 'update';
        Logger.log("Action: update");
      }

      if (action) {
        // Collect information about the modified cell
        var cellModification = {
          row: cellRow,
          column: cellCol,
          value: action === 'delete' ? null : editedValue, // If delete, set value to null
          operation: action
        };

        Logger.log("Cell Modification: " + JSON.stringify(cellModification));

        modifiedCells.push(cellModification);
      }
    }
  }

  // Build the final JSON object
  if (modifiedCells.length > 0) {
    var jsonPayload = {
      sheet_name: sheetName,
      cells: modifiedCells
    };

    Logger.log("JSON Payload: " + JSON.stringify(jsonPayload)); // Log the JSON object

    sendPostRequest(jsonPayload); // Function to send the JSON payload to the server
  } else {
    Logger.log("No modifications detected");
  }
}

// Function to send the JSON data via POST request to Kafka endpoint
function sendPostRequest(payload) {
  Logger.log("Publishing to Kafka");

  var url = "https://unique-powerful-husky.ngrok-free.app/kafka-publish-endpoint"; // Kafka publishing endpoint

  var options = {
    method: "POST",
    contentType: "application/json",
    payload: JSON.stringify(payload)
  };

  try {
    var response = UrlFetchApp.fetch(url, options);
    Logger.log("Kafka Publish Response: " + response.getContentText());
  } catch (error) {
    Logger.log("Error in Kafka publish request: " + error.toString());
  }
}



