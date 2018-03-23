const Promise = require('bluebird');
const AWS = require('aws-sdk');
const SNS = new AWS.SNS();

exports.handler = handleIt;

/**
 * Lambda endpoint handler
 */
function handleIt(event, context, callback) {
	console.log("EVENT", event);
	const response = {
		statusCode: 200,
		isBase64Encoded: false
	};
	let responseBody;
	const validationTokenParam = 'validationtoken';
	const parameters = event.queryStringParameters ? Object.keys(event.queryStringParameters) : [];

	// handle subscription validation
	if (parameters.indexOf(validationTokenParam) > -1) {
		console.log("--- VALIDATION");
		responseBody = event.queryStringParameters[validationTokenParam];
		response.body = responseBody;
		callback(null, response);
	}
	// notifications
	else {
		console.log("--- NOTIFICATION");
		const headers = event.headers;
		console.log("--- HEADERS", headers);
		const requestBody = JSON.parse(event.body);
		console.log("--- RQ BODY", requestBody);

		// TODO is this if/else block needed?
		if (!requestBody.value || requestBody.value.length === 0) {
			throw "Resource data missing";
		}
		else {
			if (requestBody.value && requestBody.value.length > 1)
				console.log("MORE THAN 1 RESOURCE DATA !!!", requestBody.value);
		}

		const userId = headers.ClientState;
		const resourceData = requestBody.value[0].ResourceData;
		if (resourceData) { // Resource Data exists
			if (resourceData.InternetMessageId) { // Resource Data has all the properties
				console.log("--- RQ RESOURCE DATA (FULL)", resourceData);
				if (userId) { // If userId (ClientState) is passed
					console.log("--- HAS UserId", userId);
					upsertMongo(userId, resourceData);
				}
				else {
					console.log("--- NO UserId");
					queryRedis(event);
				}
			}
			else { // Resource Data only contains Id property
				console.log("--- RQ RESOURCE DATA (PARTIAL)", resourceData);
				// TODO Just logging because not sure what to do here. Saw these for delete change types.
			}
		}
		else { // Like Change Type = Missed
			console.log("--- NO RESOURCE DATA");
			// send to office api request lambda
			queryOfficeApi(userId, event);
		}

		response.body = JSON.stringify(responseBody);
		callback(null, response);
	}
}

/**
 * SNS redis lambda notifier
 * @param {Object} event
 */
function queryRedis(event) {
	console.log('Lambda transfer: queryRedis');
	const payload = {
		event: event
	}
	const params = {
		Message: JSON.stringify(payload),
		Subject: "Get Redis info",
		TopicArn: "arn:aws:sns:us-west-1:931736494797:ci-redis-notify"
	};

	console.log('--- QUERY REDIS PUBLISH PARAMS', params);
	const queryRedis = SNS.publish(params, (err, data) => {
		if (err) {
			console.log("Query Redis SNS Error: ", err);
		}
		console.log("Query Redis SNS Success: ", data);
	});
	//queryRedis.send();
}

/**
 * SNS mongo lambda notifier
 * @param {string} userId
 * @param {Object} message
 */
function upsertMongo(userId, resourceData) {
	console.log('Lambda transfer: upsertMongo');
	const payload = {
		userId: userId,
		resourceData: resourceData
	};
	const params = {
		Message: JSON.stringify(payload),
		Subject: "Update mgDraft",
		TopicArn: "arn:aws:sns:us-west-1:931736494797:ci-draft-notify"
	};

	console.log('--- UPSERT MG PUBLISH PARAMS', params);
	const upsertMongo = SNS.publish(params, (err, data) => {
		if (err) {
			console.log("Upsert Mongo SNS Error: ", err);
		}
		console.log("Upsert Mongo SNS Success: ", data);
	});
	//upsertMongo.send();
}

/**
 * SNS office rest api lambda notifier
 * @param {string} userId
 * @param {Object} message
 */
function queryOfficeApi(userId, event) {
	console.log('Lambda transfer: queryOfficeApi');
	const payload = {
		userId: userId,
		event: event
	};
	const params = {
		Message: JSON.stringify(payload),
		Subject: "Find missing push notification",
		TopicArn: "arn:aws:sns:us-west-1:931736494797:ci-office-rest"
	};

	console.log('--- QUERY OFFICE API PUBLISH PARAMS', params);
	const queryOfficeApi = SNS.publish(params, (err, data) => {
		if (err) {
			console.log("Office API SNS Error: ", err);
		}
		console.log("Office API SNS Success: ", data);
	});
	//queryOfficeApi.send();
}
