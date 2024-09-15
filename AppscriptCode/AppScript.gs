function doPost(e) {
  try {
    // Extract the POST payload
    var jsonData = JSON.parse(e.postData.contents);
    
    // Process the data, for example, updating the sheet with the new values
    var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(jsonData.sheetName);
    var row = jsonData.row;
    var column = jsonData.column;
    var value = jsonData.value;

    // Update the Google Sheet with the new data
    sheet.getRange(row, column).setValue(value);

    // Optionally, publish the update to Kafka
    publishUpdateToKafka(jsonData);

    // Return success response
    return ContentService.createTextOutput(JSON.stringify({status: 'success'})).setMimeType(ContentService.MimeType.JSON);
    
  } catch (error) {
    Logger.log('Error in doPost: ' + error.toString());
    return ContentService.createTextOutput(JSON.stringify({status: 'error', message: error.toString()})).setMimeType(ContentService.MimeType.JSON);
  }
}

function doGet(e) {
  try {
    // For example, retrieve data from a specific sheet
    var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('Sheet1'); // Change the sheet name as required
    var data = sheet.getDataRange().getValues();

    // Convert data to JSON format
    var jsonData = JSON.stringify(data);
    
    // Return the data as JSON
    return ContentService.createTextOutput(jsonData).setMimeType(ContentService.MimeType.JSON);
    
  } catch (error) {
    Logger.log('Error in doGet: ' + error.toString());
    return ContentService.createTextOutput(JSON.stringify({status: 'error', message: error.toString()})).setMimeType(ContentService.MimeType.JSON);
  }
}

function publishUpdateToKafka(data) {
  var kafkaUrl = 'https://example.com/kafka-publish-endpoint'; // Replace with your Kafka publish endpoint

  var options = {
    'method': 'post',
    'contentType': 'application/json',
    'payload': JSON.stringify(data)
  };

  try {
    var response = UrlFetchApp.fetch(kafkaUrl, options);
    Logger.log('Published update to Kafka: ' + response.getContentText());
  } catch (error) {
    Logger.log('Error publishing update to Kafka: ' + error.toString());
  }
}


function consumeKafkaUpdates() {
  var kafkaUrl = 'https://example.com/kafka-consume-endpoint'; // Replace with your Kafka consume endpoint

  try {
    var response = UrlFetchApp.fetch(kafkaUrl);
    var updates = JSON.parse(response.getContentText());

    // Process the updates
    updates.forEach(function(update) {
      var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(update.sheetName);
      var row = update.row;
      var column = update.column;
      var value = update.value;

      // Update the Google Sheet with the consumed Kafka data
      sheet.getRange(row, column).setValue(value);
    });

    Logger.log('Consumed updates from Kafka');
    
  } catch (error) {
    Logger.log('Error consuming updates from Kafka: ' + error.toString());
  }
}
