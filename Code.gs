/**
 * Börse Frankfurt 4 Google Sheets
 * 
 * This Google Apps Script was derived from bf4py https://github.com/joqueka/bf4py 
 * and can retrieve historical values and current prices.
 * 
 */
function BF4GSConnector(salt) {

  // Implement logic to fetch and extract salt
  this.getSalt = function() {

      // Step 1: Get Homepage and extract main-es2015 JavaScript file
    var homepageResponse = UrlFetchApp.fetch('https://www.boerse-frankfurt.de/');
    if (homepageResponse.getResponseCode() != 200) {
      throw new Error('Could not connect to boerse-frankfurt.de');
    }
    var homepageContent = homepageResponse.getContentText();
    var fileMatch = homepageContent.match(/src="(main\.\w*\.js)/);
    if (!fileMatch || fileMatch.length < 2) {
      throw new Error('Could not find ECMA Script name');
    }
    var fileUrl = 'https://www.boerse-frankfurt.de/' + fileMatch[1];
    
    // Step 2: Get Javascript file and extract salt
    var jsResponse = UrlFetchApp.fetch(fileUrl);
    if (jsResponse.getResponseCode() != 200) {
      throw new Error('Could not connect to boerse-frankfurt.de');
    }
    var jsContent = jsResponse.getContentText();
    var saltMatch = jsContent.match(/salt:"(\w*)/);
    if (!saltMatch || saltMatch.length < 2) {
      throw new Error('Could not find tracing-salt');
    }
    return saltMatch[1];
  };

  // Implement logic to create IDs
  this._create_ids = function(url) {
      // Get the current time in UTC
      var timeutc = new Date();
      var timestr = Utilities.formatDate(timeutc, "GMT", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

      // Generate the traceidbase
      var traceidbase = timestr + url + this.salt;
      var traceid = Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, traceidbase, Utilities.Charset.UTF_8);

      // Generate xsecuritybase
      var timelocal = new Date();
      var xsecuritybase = Utilities.formatDate(timelocal, "GMT", "yyyyMMddHHmm");
      var xsecurity = Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, xsecuritybase, Utilities.Charset.UTF_8);

      // Convert the binary hash to a hexadecimal string
      traceid = traceid.map(function(byte) {
        return ('0' + (byte & 0xFF).toString(16)).slice(-2);
      }).join('');
      xsecurity = xsecurity.map(function(byte) {
        return ('0' + (byte & 0xFF).toString(16)).slice(-2);
      }).join('');

      return {
        'client-date': timestr,
        'x-client-traceid': traceid,
        'x-security': xsecurity
      };

  };

  this._get_data_url = function(functionName, params) {
    var baseurl = "https://api.boerse-frankfurt.de/v1/data/";
    var p_string = Object.keys(params).map(function(key) {
      return encodeURIComponent(key) + "=" + encodeURIComponent(params[key]);
    }).join("&");
    return baseurl + functionName + '?' + p_string;
  };

  this.data_request = function(functionName, params) {
    // Build the URL using the given function and parameters
    var url = this._get_data_url(functionName, params);

    // Create the necessary IDs
    var headers = this._create_ids(url);

    // Add additional headers
    headers['accept'] = 'application/json, text/plain, */*';

    // Set up the request options
    var options = {
      'method': 'get',
      'headers': headers,
      'muteHttpExceptions': true, // Handle exceptions within the code
      'followRedirects': true
    };

    // Make the request
    var response = UrlFetchApp.fetch(url, options);

    // Check for a successful response
    if (response.getResponseCode() !== 200) {
      var errorDetails = response.getContentText();
      throw new Error('Request to Boerse Frankfurt with url ' + url + ' failed with response code ' + response.getResponseCode() + '. Details: ' + errorDetails);
    }

    // Parse the JSON response
    var data = JSON.parse(response.getContentText());

    // Check for any messages in the response
    if (data.messages) {
      throw new Error('Boerse Frankfurt did not process request: ' + data.messages.join(', '));
    }

    return data;
  };


  this.eod_data = function(min_date, max_date, isin, mic = 'XETR') {
    if (isin === null || isin === undefined) {
      throw new Error('No ISIN given');
    }

    const [newMic, newIsin] = isin.includes(':') ? isin.split(':') : [mic, isin];

    var date_delta = (new Date(max_date) - new Date(min_date)) / (1000 * 60 * 60 * 24);
    var params = {
      'isin': newIsin,
      'mic': newMic,
      'minDate': min_date,
      'maxDate': max_date,
      'limit': date_delta,
      'cleanSplit': false,
      'cleanPayout': false,
      'cleanSubscription': false
    };

    var data = this.data_request('price_history', params);

    return data.data;
  };

  /*
  * Returns the most recent data for the given ISIN at the specified stock exchange.
  *
  * @param {string} isin - The International Securities Identification Number (ISIN) representing the desired financial instrument.
  * @param {string} [mic='XETR'] - The Market Identifier Code (MIC) representing the desired stock exchange. Defaults to 'XETR'.
  *
  * @throws {Error} If the provided ISIN is null or undefined.
  *
  * @returns {object} An object containing the following details:
  *  - isin {string}: The ISIN of the instrument.
  *  - bidLimit {number} (currency): The current bid price.
  *  - askLimit {number} (currency): The current ask price.
  *  - bidSize {integer}: The size of the current bid in terms of the number of stocks offered.
  *  - askSize {number} (integer): The size of the current ask in terms of the number of stocks offered.
  *  - lastPrice {number} (currency): The last traded price (Warning: This is rounded by Boerse Frankfurt)
  *  - avgLastPrice {number} (currency): The average of the bid and ask prices.
  *  - timestampLastPrice {string}: Timestamp of the last traded price.
  *  - changeToPrevDayAbsolute {number} (currency): Absolute change compared to the previous trading day.
  *  - changeToPrevDayInPercent {number}: Relative change in percentage compared to the previous trading day.
  *  - spreadAbsolute {number} (currency): Absolute value of the spread.
  *  - spreadRelative {number}: Relative value of the spread in percentage.
  *  - timestamp {string}: Timestamp of the data.
  *  - nominal {boolean}: Indicates whether the data is nominal.
  *  - tradingStatus {string}: Current trading status (e.g., 'PRE_CALL').
  *  - instrumentStatus {string}: Current status of the instrument (e.g., 'ACTIVE').
  *  - open {number} (currency): Opening price for the day.
  */
  this.quote_box_single = function(isin, mic = 'XETR') {
    if (!isin) throw new Error('No ISIN given');

    const [newMic, newIsin] = isin.includes(':') ? isin.split(':') : [mic, isin];
    const params = { 'isin': newIsin, 'mic': newMic };
    const data = this.data_request('quote_box/single', params);
    console.log(data);

    data.avgLastPrice = (data.bidLimit + data.askLimit) / 2;
    return data;
  };

  this.session = UrlFetchApp;
  this.salt = salt || this.getSalt();

}

function updateHistoricalPrices(connector, isins, sheet) {
  var lastColumn = sheet.getLastColumn();
  var range = sheet.getRange(1, 1, sheet.getLastRow(), lastColumn);
  var values = range.getValues();
  var lastRow = values[values.length - 1];
  var lastDateInSheet = lastRow[0];
  var currentDate = new Date(lastDateInSheet);
  var yesterday = new Date();
  yesterday.setHours(0,0,0,0);
  yesterday.setDate(yesterday.getDate() - 1);
  var missingDates = [];

  var formattedLastRowDate = Utilities.formatDate(new Date(lastDateInSheet), SpreadsheetApp.getActiveSpreadsheet().getSpreadsheetTimeZone(), "yyyy-MM-dd");
  var formattedYesterday   = Utilities.formatDate(new Date(yesterday), SpreadsheetApp.getActiveSpreadsheet().getSpreadsheetTimeZone(), "yyyy-MM-dd");

  if ((formattedLastRowDate == formattedYesterday) && lastRow[1].indexOf("EOD") == -1)
  {
    sheet.deleteRow(sheet.getLastRow())
    range = sheet.getRange(1, 1, sheet.getLastRow(), lastColumn);
    values = range.getValues();
    lastRow = values[values.length - 1];
    lastDateInSheet = lastRow[0];
    currentDate = new Date(lastDateInSheet);
  }

  while (currentDate < yesterday) {
    currentDate.setDate(currentDate.getDate() + 1);
    missingDates.push(new Date(currentDate));
  }

  // Return if there are no missing dates
  if (missingDates.length === 0) return;

  var allData = {};

  // Initialize previous closing prices with the last known value for each ISIN
  var prevClosePrices = isins.map((isin, index) => {
    var lastValue = lastRow[index + 2]; // +2 because the ISINs start from the third column
    return !isNaN(+lastValue) ? lastValue : "NA";
  });

  isins.forEach((isin, colIndex) => {

    minDateDate = new Date();
    minDateDate.setDate(missingDates[0] - 1);

    var minDate = Utilities.formatDate(minDateDate, SpreadsheetApp.getActiveSpreadsheet().getSpreadsheetTimeZone(), "yyyy-MM-dd");
    var maxDate = Utilities.formatDate(yesterday, SpreadsheetApp.getActiveSpreadsheet().getSpreadsheetTimeZone(), "yyyy-MM-dd");
    var data = connector.eod_data(minDate, maxDate, isin);

    missingDates.forEach((date, index) => {
      var formattedDate = Utilities.formatDate(date, SpreadsheetApp.getActiveSpreadsheet().getSpreadsheetTimeZone(), "yyyy-MM-dd");
      var closePrice;
      var entry = data && data.find(entry => entry.date === formattedDate);
      if (entry) {
        closePrice = entry.close;
        prevClosePrices[colIndex] = closePrice;
      } else {
        closePrice = prevClosePrices[colIndex];
      }

      if (!allData[formattedDate]) {
        allData[formattedDate] = [formattedDate, entry ? "EOD" : "EOD PrevDay"];
      }
      allData[formattedDate][colIndex + 2] = closePrice;
    });
  });

  missingDates.forEach((date) => {
    var formattedDate = Utilities.formatDate(date, SpreadsheetApp.getActiveSpreadsheet().getSpreadsheetTimeZone(), "yyyy-MM-dd");
    var row = allData[formattedDate];
    if (row) {
      sheet.appendRow(row);
      sheet.getRange(sheet.getLastRow(), 3, 1, isins.length).setNumberFormat('#,##0.00');
      sheet.getRange(sheet.getLastRow(), 1, 1, 2).setNumberFormat('@');
    }
  }); 
}

function updateCurrentDay(connector, isins, sheet) {
  var today = new Date();
  var formattedToday = Utilities.formatDate(today, SpreadsheetApp.getActiveSpreadsheet().getSpreadsheetTimeZone(), "yyyy-MM-dd");
  var rowForToday = [formattedToday, null];
  
  isins.forEach((isin) => {
    try {
      var quoteData = connector.quote_box_single(isin);
      rowForToday.push(quoteData.lastPrice);
      if(rowForToday[1] == null)
      {
        rowForToday[1] = quoteData.timestampLastPrice
      }
    } catch (error) {
      rowForToday.push("#NA");
    }
  });

  var lastRowDate = sheet.getRange(sheet.getLastRow(), 1).getValue();
  var formattedLastRowDate = Utilities.formatDate(new Date(lastRowDate), SpreadsheetApp.getActiveSpreadsheet().getSpreadsheetTimeZone(), "yyyy-MM-dd");

  if (formattedLastRowDate === formattedToday) {
    sheet.getRange(sheet.getLastRow(), 1, 1, rowForToday.length).setValues([rowForToday]);
  } else {
    sheet.appendRow(rowForToday);
    sheet.getRange(sheet.getLastRow(), 1, 1, 2).setNumberFormat('@');
    sheet.getRange(sheet.getLastRow(), 3, 1, isins.length).setNumberFormat('#,##0.00');
  }
}

function hideTopRows(sheet, n, headerRows = 1) {
  var totalRows = sheet.getLastRow();
  
  // Calculate the first row to hide (1-indexed).
  var firstRowToHide = headerRows + 1;
  
  // Calculate the number of rows to hide.
  var rowsToHide = totalRows - n - headerRows;

  // Hide the rows from the first row after the header to the row before the last n rows.
  if (rowsToHide > 0) {
    sheet.hideRows(firstRowToHide, rowsToHide);
  }
}

function myOnOpenForTrigger() {
  var connector = new BF4GSConnector();
  var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('HistoricalPrices');
  var isins = sheet.getRange(1, 3, 1, sheet.getLastColumn() - 2).getValues()[0];
  

  updateHistoricalPrices(connector, isins, sheet);
  updateCurrentDay(connector, isins, sheet);
  hideTopRows(sheet, 14);
}

function test(){
  
  var connector = new BF4GSConnector();
  console.log(connector.quote_box_single("IE00BQT3WG13"))

  console.log(connector.quote_box_single("CA82509L1076", "XFRA"))
}

function testGetMarketStatus(){
  console.log(getMarketStatus())
}

function getMarketStatus() {
  const url = 'https://www.tradinghours.com/open?';
  const response = UrlFetchApp.fetch(url);
  const html = response.getContentText();

  // Regular expressions to find the market status
  const openRegex = /<span class="[^"]*text-open[^"]*">\s*<u>Yes<\/u>\s*<\/span>/;
  const closedRegex = /<span class="[^"]*text-closed[^"]*">\s*<u>No<\/u>\s*<\/span>/;

  if (openRegex.test(html)) {
    return 'US markets are open today';
  } else if (closedRegex.test(html)) {
    return 'US markets are closed today';
  } else {
    return 'Status not found';
  }
}

function test2(){

  var connector = new BF4GSConnector();

  var min_date = '2023-11-06';
  var max_date = '2023-11-07';
  var isin = 'US0378331005'; // 'FR0010315770';
  
  console.log(connector.quote_box_single(isin))

  var endOfDayData = connector.eod_data(min_date, max_date, isin);

  // Log or further process the data as needed
  console.log(endOfDayData);
}
