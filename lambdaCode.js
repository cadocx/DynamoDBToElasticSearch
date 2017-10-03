const AWS = require('aws-sdk');
const path = require('path');

//Change those with your endpoints and index names
const esDomain = {
    region: "us-east-1",
    endpoint: "",
    index: "",
    doctype: "onboardingrecords"
};

const endpoint = new AWS.Endpoint(esDomain.endpoint);
const creds = new AWS.EnvironmentCredentials('AWS');

exports.handler = (event, context, callback) =>{
    //console.log('Received event:', JSON.stringify(event, null, 2));
    console.log(JSON.stringify(esDomain));
    console.log(event);
    event.Records.forEach((record) =>{
        console.log(record.eventID);
        console.log(record.eventName);
        record.dynamodb.NewImage = mapper(record.dynamodb.NewImage);
        console.log('DynamoDB Record: %j', record.dynamodb);
       
        const dbRecord = JSON.stringify(record.dynamodb);
        postToES(dbRecord, context, callback);
    });
};

function mapper(data) {
    let S = "S";
    let SS = "SS";
    let NN = "NN";
    let NS = "NS";
    let BS = "BS";
    let BB = "BB";
    let N = "N";
    let BOOL = "BOOL";
    let NULL = "NULL";
    let M = "M";
    let L = "L";

    if (isObject(data)) {
        let keys = Object.keys(data);
        while (keys.length) {
            let key = keys.shift();
            let types = data[key];

            if (isObject(types) && types.hasOwnProperty(S)) {
                data[key] = types[S];
            } else if (isObject(types) && types.hasOwnProperty(N)) {
                data[key] = parseFloat(types[N]);
            } else if (isObject(types) && types.hasOwnProperty(BOOL)) {
                data[key] = types[BOOL];
            } else if (isObject(types) && types.hasOwnProperty(NULL)) {
                data[key] = null;
            } else if (isObject(types) && types.hasOwnProperty(M)) {
                data[key] = mapper(types[M]);
            } else if (isObject(types) && types.hasOwnProperty(L)) {
                data[key] = mapper(types[L]);
            } else if (isObject(types) && types.hasOwnProperty(SS)) {
                data[key] = types[SS];
            } else if (isObject(types) && types.hasOwnProperty(NN)) {
                data[key] = types[NN];
            } else if (isObject(types) && types.hasOwnProperty(BB)) {
                data[key] = types[BB];
            } else if (isObject(types) && types.hasOwnProperty(NS)) {
                data[key] = types[NS];
            } else if (isObject(types) && types.hasOwnProperty(BS)) {
                data[key] = types[BS];
            }
        }
    }


    return data;

    function isObject(value) {
        return typeof value === "object" && value !== null;
    }
}



function postToES(doc, context, lambdaCallback){
    const req = new AWS.HttpRequest(endpoint);

    req.method = 'POST';
    req.path = path.join('/', esDomain.index, esDomain.doctype);
    req.region = esDomain.region;
    req.headers['presigned-expires'] = false;
    req.headers['Host'] = endpoint.host;
    req.body = doc;

    const signer = new AWS.Signers.V4(req , 'es');
    signer.addAuthorization(creds, new Date());

    const send = new AWS.NodeHttpClient();
    send.handleRequest(req, null, function(httpResp){
        let respBody = '';
        httpResp.on('data', function (chunk) {
            respBody += chunk;
        });
        httpResp.on('end', function (chunk) {
            console.log('Response: ' + respBody);
            lambdaCallback(null,'Lambda added document ' + doc);
        });
    }, function(err) {
        console.log('Error: ' + err);
        lambdaCallback('Lambda failed with error ' + err);
    });
}