const Promise = require('bluebird');
const AWS = require('aws-sdk');
const SNS = new AWS.SNS();

exports.handler = handler;

/**
 * Lambda endpoint handler
 */
function handler(event, context, callback) {
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
		responseBody = event.queryStringParameters[validationTokenParam];
		response.body = responseBody;
		callback(null, response);
	}
	// notifications
	else {
		const headers = event.headers;
		const requestBody = JSON.parse(event.body);

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
			if (userId) { // If userId (ClientState) is passed
				upsertMongo(userId, resourceData);
			}
			else {
				queryRedis(event);
			}
		}
		else { // Like Change Type = Missed
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

	const queryRedis = SNS.publish(params);
	queryRedis.send();
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

	const upsertMongo = SNS.publish(params);
	upsertMongo.send();
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

	const queryOfficeApi = SNS.publish(params);
	queryOfficeApi.send();
}
