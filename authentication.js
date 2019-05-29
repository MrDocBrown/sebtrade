const fetch = require('node-fetch');
const functions = require('firebase-functions');

const url = "https://api.ig.com/gateway/deal";
const user = functions.config().ig.user;
const pw = functions.config().ig.pw;
const api_key = functions.config().ig.api_key;

// obtaining the token from IG and passing it via a promise
exports.obtainToken = async () => {
    try {
        const response = await fetch(`${url}/session`, {
            method: "POST", 
            headers: {
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "VERSION": "3",
                    "X-IG-API-KEY": api_key
                },
            body: JSON.stringify({"identifier":user, "password":pw})
            })
        const jsonResponse = await response.json();
        const access_token = jsonResponse.oauthToken.access_token;
        return access_token;
    } catch (error) {
        console.log('Fetch Error :-S', error);
  };
};