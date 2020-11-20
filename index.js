// -- Importing all neccesary libraries for the parser --
const moment = require('moment');
var startTime = moment().format("LTS")
const axios = require('axios');
const fileStream = require("fs");
const AWS = require('aws-sdk')
const s3 = new AWS.S3();

// -- Declaring all API Access tokens and URLs --
const CLIENT_ID = process.env.CLIENT_ID;
const CLIENT_SECRET = process.env.CLIENT_SECRET;
const REFRESH_TOKEN = process.env.REFRESH_TOKEN;
const CLIENT_FOLDER = process.env.CLIENT_FOLDER;
const AUTH_URL = 'https://accounts.zoho.com/oauth/v2/token';
const AUTH_PARAMS = `?refresh_token=${REFRESH_TOKEN}&grant_type=refresh_token&client_id=${CLIENT_ID}&client_secret=${CLIENT_SECRET}`;
exports.handler = function (event){
    var getFile = async () => {
        try{
            let params = {
                Bucket : 'ivansfiles',
                Key : `${CLIENT_FOLDER}/awsdata.json`
            }
            const data = await s3.getObject(params).promise();
            console.log(JSON.parse(data.Body.toString())['data'])
            await API_CALL_ASYNC((JSON.parse(data.Body.toString()))["data"])
        }
        catch(e){
            throw new Error(`Could not retrieve File ${e.message}`);
        }
    }

    // -- Async function for API calls so that API calls are being executed in order and 
    // not all at once
    const API_CALL_ASYNC = async (JSON_FILE_DATA) => {

        // -- API call to get API authentication token
        var authApiResponse = await axios({
            method: "POST",
            url: `${AUTH_URL}${AUTH_PARAMS}`
        })
        // Assigning the token to a variable - API_TOKEN - for later use
        const API_TOKEN = authApiResponse.data.access_token
        

        // - After reading the JSON data, iterate through all objects and run parser for each object
        for (var i = 0; i < JSON_FILE_DATA.length; i++) {
            //API call to get all data that matches with the search?criteria=((Name))
            var apiCall = await axios({
                method : "GET",
                url : `https://www.zohoapis.com/crm/v2/siegeamsextensionv10__Policies/search?criteria=((Name:equals:${JSON_FILE_DATA[i].Name}))`,
                headers : {
                    "Authorization": `Bearer ${API_TOKEN}`
                }
            })


            
            var apiReturnObject = apiCall.data.data
            

            // Condition 1: The API name is not found in the return object
            // Action : Create new record and insert it into ZOHO CRM with a field showing "Unmatched" as true
            if(apiReturnObject === undefined){
                
                //For insertions, create an object and populate it with the correct data to put into ZOHO CRM
                var accountObject = {}
                var jsonTempObject = {}
                jsonTempObject.data = []
                accountObject["Account_Name"] = JSON_FILE_DATA[i].Insured_Name

                jsonTempObject.data.push(accountObject)
                var insertAccount = await axios({
                    method : "POST",
                    url : `https://www.zohoapis.com/crm/v2/Accounts`,
                    data : jsonTempObject,
                    headers : {
                        "Authorization": `Bearer ${API_TOKEN}`
                    }
                })
                var contactObject = {}
                accountObject = {}
                jsonTempObject = {}
                jsonTempObject.data = []
                
                contactObject["Last_Name"] = JSON_FILE_DATA[i].Insured_Name
                contactObject["Account_Name"] = {}
                contactObject["Account_Name"].name = JSON_FILE_DATA[i].Insured_Name
                contactObject["Account_Name"].id = insertAccount.data.data[0].details.id

                jsonTempObject.data.push(contactObject)

                var insertContact = await axios({
                    method : "POST",
                    url : `https://www.zohoapis.com/crm/v2/Contacts`,
                    data : jsonTempObject,
                    headers : {
                        "Authorization": `Bearer ${API_TOKEN}`
                    }
                })


                jsonTempObject.data = []
                // Add an unmatched key with a value of true since we could not find the policy number or name in ZOHO
                JSON_FILE_DATA[i].siegeamsextensionv10__Unmatched = true
                JSON_FILE_DATA[i].siegeamsextensionv10__Last_Update_by_Download = moment().format().toString()
                JSON_FILE_DATA[i].Account = {}
                JSON_FILE_DATA[i].Account.name = JSON_FILE_DATA[i].Insured_Name
                JSON_FILE_DATA[i].Account.id = insertAccount.data.data[0].details.id
                JSON_FILE_DATA[i].Contact = {}
                JSON_FILE_DATA[i].Contact.name = JSON_FILE_DATA[i].Insured_Name
                JSON_FILE_DATA[i].Contact.id = insertContact.data.data[0].details.id

                jsonTempObject.data.push(JSON_FILE_DATA[i])

                // Insertion API call wit the jsonTempObject that we made
                var insertApiCall = await axios({
                    method :"POST",
                    url : `https://www.zohoapis.com/crm/v2/siegeamsextensionv10__Policies`,
                    data : jsonTempObject,
                    headers : {
                        "Authorization": `Bearer ${API_TOKEN}`
                    }
                })
                jsonTempObject = {}
            }
            // Condition 2 : Policy number was found 
            // Action : See if the effective date are found 
            else{
                // Variable that get triggered to true if policy effective date is found
                let didFindPolicyEffectiveDate = false

                // Iterating through all policies with the matching policy number and seeing if any of them match the effective date 
                for(var j = 0;j< apiReturnObject.length;j++){
                    // Condition 2.1 : Matching effective date found
                    // Checking if the effective date in the returned object matches with the effective date in the JSON data that we are reading
                    if(apiReturnObject[j].Effective_Date == JSON_FILE_DATA[i].Effective_Date){

                        // creating object to populate for API data
                        var jsonTempObject = {}
                        jsonTempObject.data = []
                        jsonTempObject.data.push(JSON_FILE_DATA[i])


                        // Calling update API with the jsonTempObject data
                        var updateApiCall = await axios({
                            method :"PUT",
                            url : `https://www.zohoapis.com/crm/v2/siegeamsextensionv10__Policies/${apiReturnObject[0].id}`,
                            data : jsonTempObject,
                            headers : {
                                "Authorization": `Bearer ${API_TOKEN}`
                            }
                        })
                        jsonTempObject = {}
                        // Trigger the didFindPolicyEffectiveDate to true so that we know effective date and policy numbers have been matched
                        didFindPolicyEffectiveDate = true
                        break
                    }
                }

                // Condition 2.2 : Matching effective date not found
                if(!didFindPolicyEffectiveDate){

                    // Set a default date of 1900s to compare and get the latest date in the returning data from ZOHO CRM
                    var latestDate = "1900-01-01"
                    var latestIndex = 0

                    // Iterate through all policies that match the policy name and get the object with the latest Effective date. 
                    for(var k = 0;k<apiReturnObject.length;k++){
                        // Compare and see if the Effective date on the object[k] is later than the 'latestDate' variable

                        if(moment(apiReturnObject[k].Effective_Date).isAfter(latestDate,'day')){
                            // If found, set the latestDate to the date of return object
                            latestDate = apiReturnObject[k].Effective_Date
                            // Set the index of the object for later use
                            latestIndex = k
                        }
                    }



                    var jsonTempObject = {}
                    jsonTempObject.data = []

                    // Create objects for Account and Contact info

                    var accountObject = {}
                    accountObject.Account = {}
                    var contactObject = {}
                    contactObject.Account = {}

                    // Populate Account and Contact object 
                    try{
                        accountObject.name  = apiReturnObject[latestIndex].Account.name
                        accountObject.id  = apiReturnObject[latestIndex].Account.id
                        contactObject.name  = apiReturnObject[latestIndex].Contact.name
                        contactObject.id  = apiReturnObject[latestIndex].Contact.id
                    }
                    catch(e){
                        accountObject.name = JSON_FILE_DATA[i].Name
                        JSON_FILE_DATA[i].Account = accountObject
            

                        contactObject.Account.name = JSON_FILE_DATA[i].Name
                        JSON_FILE_DATA.Contact = contactObject
                    }

                    // Insert the updated date into the JSON data
                    JSON_FILE_DATA[i].Account = accountObject
                    JSON_FILE_DATA[i].Contact = contactObject
                    JSON_FILE_DATA[i].siegeamsextensionv10__Last_Update_by_Download = moment().format().toString()

                    // Push data to JSON data
                    jsonTempObject.data.push(JSON_FILE_DATA[i])

                    // Call insertion API 
                    var insertApiCall = await axios({
                        method :"POST",
                        url : `https://www.zohoapis.com/crm/v2/siegeamsextensionv10__Policies`,
                        data : jsonTempObject,
                        headers : {
                            "Authorization": `Bearer ${API_TOKEN}`
                        }
                    })
                    jsonTempObject = {}
                }

            }
        }
    } 
    getFile()
}
