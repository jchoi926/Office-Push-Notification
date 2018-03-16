const AWS = require('aws-sdk');
const Promise = require('bluebird');
const md5 = require('md5');

let env;
let config = {};
let redisClient;

exports.handler = handleIt;

/**
 * Lambda endpoint handler
 */
function handleIt(event, context, callback) {
	console.log("EVENT", event);
	env = event.requestContext.stage;
	const parameters = event.queryStringParameters ? Object.keys(event.queryStringParameters) : [];
	const validationTokenParam = 'validationtoken';

	const response = {
		statusCode: 200,
		/*headers: {
			'Content-Type': 'text/plain'
		},*/
		isBase64Encoded: false
	};
	let responseBody;

	// handle subscription validation
	if (parameters.indexOf(validationTokenParam) > -1) {
		console.log("SUBSCRIPTION VALIDATION ---");
		responseBody = event.queryStringParameters[validationTokenParam];
		response.body = responseBody;
		callback(null, response);
	}
	// notifications
	else {
		console.log("NOTIFICATION ---");
		const headers = event.headers;
		const requestBody = JSON.parse(event.body);
		console.log("REQUEST-HEADERS", headers);
		console.log("REQUEST_BODY", requestBody);

		initialize()
			.then(() => {
				console.log('Lambda initialized');

				if (!requestBody.value || requestBody.value.length === 0) {
					console.log("RESOURCE DATA MISSING !!!", requestBody.value);
					throw "Resource data missing";
				} else {
					if (requestBody.value && requestBody.value.length > 1)
						console.log("MORE THAN 1 RESOURCE DATA !!!", requestBody.value);
				}

				const userId = headers.ClientState;
				const resourceData = requestBody.value[0].ResourceData;
				console.log("REQUEST_RESOURCE_DATA", resourceData);
				if (resourceData) { // Resource Data exists
					if (userId) { // If userId (ClientState) is passed
						console.log('USING CLIENT STATE USER ID', userId);
						sendToDraft(userId, resourceData);
					} else {
						// Get User Id from Redis using Subscription Id
						const subscriptionId = resourceData.SubscriptionId;
						const subscriptionIdHash = md5(subscriptionId);
						console.log('FETCH USER ID FROM EMAIL SUBSCRIPTION', subscriptionIdHash);

						redisClient.hgetallAsync(`email_subscription:${subscriptionIdHash}`)
							.then(emailSubscription => {
								console.log('EMAIL-SUBSCRIPTION', emailSubscription);
								sendToDraft(userId, resourceData);
							})
							.catch(err => {
								throw err;
							})
						;
					}
				} else // Like Change Type = Missed
					// send to office api request lambda
					console.log("!!! RESOURCE DATA MISSING !!!");

				response.body = JSON.stringify(responseBody);

				cleanUp();
				callback(null, response);
			})
			.catch(err => {
				cleanUp();
				callback(err);
			})
		;
	}
}

/**
 * Bootstrap Lambda
 * @return {Promise}
 */
function initialize() {
	return getConfigParams()
		.then(() => setRedisClient())
	;
}

function sendToDraft(userId, message) {
	const SNS = new AWS.SNS();
	const payload = {
		userId: userId,
		draft: message,
		config: config
	};
	var params = {
        Message: JSON.stringify(payload),
        Subject: "Update mgDraft",
        TopicArn: "arn:aws:sns:us-west-1:931736494797:ci-draft-notify"
    };

    const draftUpdate = SNS.publish(params);
    draftUpdate.send();
}

/**
 * Clean up
 */
function cleanUp() {
	if (redisClient) redisClient.quit();
}

/**
 * Set redis client
 * @return {Promise}
 */
function setRedisClient() {
	if (redisClient)
		return Promise.resolve(redisClient);

	return new Promise((resolve, reject) => {
		const redis = require('redis');
		Promise.promisifyAll(redis.RedisClient.prototype);
		redisClient = redis.createClient({host: config.redis.host, port: parseInt(config.redis.port), password: config.redis.pass});

		redisClient.on('connect', () => {
			console.log('Redis client connected');
			resolve(redisClient);
		})
		.on('error', (err) => {
			console.log('Redis client error', err);
			reject(err);
		});
	});
}

/**
 * Get parameters from AWS Systems Manager Parameter Store
 * @return {Promise}
 */
function getConfigParams() {
	if (Object.keys(config).length > 0)
		return Promise.resolve(config);

	const S3 = Promise.promisifyAll(new AWS.S3({region: 'us-east-1'}));
	return S3.getObjectAsync({Bucket: 'ci-office-notification', Key: `${env}.json`})
		.then(s3Obj => {
			config = JSON.parse(s3Obj.Body.toString());
			return config;
		})
	;
}
