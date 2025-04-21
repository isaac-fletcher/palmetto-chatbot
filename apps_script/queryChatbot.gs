function queryChatbot() {
  var sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
  // Paste the api url in the url variable
  var url = "https://v27nn0mx4l.execute-api.us-east-1.amazonaws.com/dev/chat";

  for (var row = 2; row <= 25; row++) { // Edit row maximum value for how many questions you have
    var inputText = sheet.getRange("B" + row).getValue();  // Message to send is in column B

    if (inputText) {
      var payload = {
        text: inputText
      };

      var options = {
        method: "post",
        contentType: "application/json",
        payload: JSON.stringify(payload),
        muteHttpExceptions: true
      };

      try {
        var response = UrlFetchApp.fetch(url, options);
        var json = JSON.parse(response.getContentText());

        if (json.result === "OK" && json.text) {
          // Storing raw Markdown response in column D
          sheet.getRange("D" + row).setValue(json.text);  // Output goes here as raw Markdown
        } else {
          sheet.getRange("D" + row).setValue("Error: " + (json.error || "Unexpected response"));
        }
      } catch (e) {
        sheet.getRange("D" + row).setValue("Exception: " + e.message);
      }
    } else {
      sheet.getRange("D" + row).setValue("No input in B" + row);  // If no input is in the cell
    }
  }
}
