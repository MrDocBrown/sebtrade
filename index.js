const token = require('./authentication');
var fetch = require('node-fetch');
const functions = require('firebase-functions');
const admin = require('firebase-admin');

admin.initializeApp();

// Airtable variables
const airtable_url = `https://api.airtable.com/v0`;
// Some variables stored in Firebase Environment (see for example: functions.config().airtable.key)
const airtable_base = functions.config().airtable.base;
const airtable_table = 'Indizes';
const airtable_key = functions.config().airtable.key;

// IG Markets variables, also stored in Firebase
const ig_url = "https://api.ig.com/gateway/deal";
const accountID = functions.config().ig.accountid;
const api_key = functions.config().ig.api_key;

// Pull Epics from the EPICS table in Airtable. This allows the user to set their own set of EPICS to pull
const pullEpicsFromAirtable = async () => {
    try {
        const response = await fetch(`${airtable_url}/${airtable_base}/EPICS`, {
            method: "GET",
            headers: {
                "Content-Type": "application/json",
                "Authorization": "Bearer " + airtable_key
            }
        });
        const res = await response.json();
        // res.records now is an array of all rows from the EPICS table 
        // Checking those that are 'TRUE' and pushing those into the Array
        const EPICS = [];
        res.records.forEach(function(record) {
            if (record.fields.Pull_Data === 'TRUE') {
                EPICS.push(record.fields.EPIC);
            } else {
                console.log(`skipping ${record.fields.EPIC} as its set to FALSE`);
                return;
            }
        })
        return EPICS;
    } catch (error) {
        console.log(error);
    }
}

// Obtain the Market ID from IG. Required to pull through Sentiment
const getMarketId = async (access_token, epic) => {
    try {
        const response = await fetch(`${ig_url}/markets/${epic}`, {
            method: "GET",
            headers: {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": "Bearer " + access_token,
                "X-IG-API-KEY": api_key,
                "IG-ACCOUNT-ID": accountID
            }
        })
        return response.json();
    } catch (error) {
        console.log(`Fetch Error: Type ${error} for ${epic}`);
    }
}

// Obtain price from EPIC
const getPrice = async (access_token, epic) => {
    try {
        const response = await fetch(`${ig_url}/markets/${epic}`, {
            method: "GET",
            headers: {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": "Bearer " + access_token,
                "X-IG-API-KEY": api_key,
                "IG-ACCOUNT-ID": accountID
            }
        })
        const jsonResponse = await response.json();
        const priceInfo = {"bid": jsonResponse.snapshot.bid, "offer": jsonResponse.snapshot.offer};
        return priceInfo;
    } catch (error) {
        console.log(`Error: Type ${error}`);
    }
}

// Obtain the Sentiment, ie. the percentage of long and short positions
const getLongShort = async (access_token, marketId) => {
    try {
        const response = await fetch(`${ig_url}/clientsentiment?marketIds=${marketId}`, {
            method: "GET",
            headers: {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": "Bearer " + access_token,
                "X-IG-API-KEY": api_key,
                "IG-ACCOUNT-ID": accountID
            }
        })
        const jsonResponse = await response.json();
        const t = jsonResponse.clientSentiments[0];
        const clientSentiments = {"short": t.shortPositionPercentage, "long": t.longPositionPercentage};
        return clientSentiments;
    } catch (error) {
        console.log(`Error: Type ${error}`);
    }
}

// Posting the price and longShort information into Airtable
const postPrice = async (priceInfo, longShort, marketId) => {
    try {
        const response = await fetch(`${airtable_url}/${airtable_base}/${airtable_table}`, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "Authorization": "Bearer " + airtable_key
            },
            body: JSON.stringify({"fields": {"Instrument": marketId, 
            "Bid": priceInfo.bid, "Offer": priceInfo.offer, "Long": longShort.long, "Short": longShort.short}})
        })
        const jsonResponse = await response.json();
        return jsonResponse;
    } catch (error) {
        console.log(`Error: ${error}`);
    }
}

// Combines all API calls from above into one single function
const mergeAndPost = async (epic) => {
    try {       
        const access_token = await token.obtainToken();
        const marketId = await getMarketId(access_token, epic);
        const priceInfo = await getPrice(access_token, epic);
        const longShort = await getLongShort(access_token, marketId.instrument.marketId);
        if(marketId.snapshot.marketStatus === "TRADEABLE")  {
            postPrice(priceInfo, longShort, marketId.instrument.marketId);
            console.log(marketId.instrument.marketId + ' IS ' + marketId.snapshot.marketStatus);
            console.log('Pushed at:', new Date());
        } else {
            console.log(`${marketId.instrument.marketId} IS ${marketId.snapshot.marketStatus}. NO PUSH`)
        }
    } catch (error) {
        console.log(`Error: ${error}`);
    }
};

// Manual function to achieve async/await for an ForEach method. The default method does not wait for one iteration to end
const waitFor = (ms) => new Promise(r => setTimeout(r, ms))
const asyncForEach = async (array, callback) => {
  for (let index = 0; index < array.length; index++) {
    await callback(array[index], index, array)
  }
}

// Combines the API calls and manual forEach method into one function. 3000ms is set so that the IG API Rate Limit is not hit
const start = async (epics) => {
  await asyncForEach(epics, async (epic) => {
    await waitFor(3000)
    await mergeAndPost(epic)
  })
}

// Finally calls the job by pulling through the information from Airtable and then runs start() with that information
const runJob = async () => {
    // Pulls epics from Airtable
    const epics = await pullEpicsFromAirtable();
    // Uses epics to run through all datapoints to pull from IG and post to Airtable "Indizes"
    start(epics);
}

// Firebase function
exports.runIGToAirtableJob = functions.pubsub.schedule('0 * * * *').onRun(async context => {
    await runJob();
    console.log(`Job initiated. Data will now push into Airtable`);
  });