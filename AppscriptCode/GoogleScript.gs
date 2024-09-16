function onEditTrigger(e) {
  // Check if the edited range contains data
  if (!e || !e.range) return;

  // Get the sheet and range information
  var sheet = e.range.getSheet();
  var range = e.range;
  var sheetName = sheet.getName();
  var rowStart = range.getRow();
  var colStart = range.getColumn();
  var numRows = range.getNumRows();
  var numCols = range.getNumColumns();

  var modifiedCells = [];

  // Iterate over all modified cells
  for (var i = 0; i < numRows; i++) {
    for (var j = 0; j < numCols; j++) {
      var cellRow = rowStart + i;
      var cellCol = colStart + j;
      var editedValue = sheet.getRange(cellRow, cellCol).getValue();
      var oldValue = e.oldValue; // Get the old value before the edit

      // Determine the type of modification (created, updated, deleted)
      var action = '';
      if (!oldValue && editedValue) {
        action = 'created';
      } else if (oldValue && !editedValue) {
        action = 'deleted';
      } else if (oldValue !== editedValue) {
        action = 'updated';
      }

      if (action) {
        // Collect information about the modified cell
        modifiedCells.push({
          sheetName: sheetName,
          row: cellRow,
          column: cellCol,
          oldValue: oldValue || null,
          newValue: editedValue || null,
          action: action
        });
      }
    }
  }

  // If any cells were modified, send the data
  if (modifiedCells.length > 0) {
    Logger.log(modifiedCells);
    sendPostRequest(modifiedCells);
  }
}

function sendPostRequest(modifiedCells) {
  var url = 'https://example.com/your-backend-endpoint';  // Replace with your backend endpoint

  var data = {
    timestamp: new Date().toISOString(),
    modifiedCells: modifiedCells
  };

  var options = {
    'method': 'post',
    'contentType': 'application/json',
    'payload': JSON.stringify(data)
  };

  try {
    var response = UrlFetchApp.fetch(url, options);
    Logger.log('POST request sent successfully: ' + response.getContentText());
  } catch (error) {
    Logger.log('Error sending POST request: ' + error.toString());
  }
}
