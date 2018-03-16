const AWS = require('aws-sdk');
const Promise = require('bluebird');
const md5 = require('md5');

let env;
let config = {};
let redisClient;
let mongoClient;
let mongoDB;

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
						processDrafts(userId, resourceData);
					} else {
						// Get User Id from Redis using Subscription Id
						const subscriptionId = resourceData.SubscriptionId;
						const subscriptionIdHash = md5(subscriptionId);
						console.log('FETCH USER ID FROM EMAIL SUBSCRIPTION', subscriptionIdHash);

						redisClient.hgetallAsync(`email_subscription:${subscriptionIdHash}`)
							.then(emailSubscription => {
								console.log('EMAIL-SUBSCRIPTION', emailSubscription);
								processDrafts(emailSubscription.user_id, resourceData);
							})
							.catch(err => {
								throw err;
							})
						;
					}
				} else // Like Change Type = Missed
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
		.then(() => {
			return Promise.all([
				setRedisClient(),
				getMongoClient()
			]);
		})
	;
}

/**
 * Clean up
 */
function cleanUp() {
	if (redisClient) redisClient.quit();
	// if (mongoClient) mongoClient.close(); // Commented to prevent MongoError: topology was destroyed
}

/**
 * Set Mongo Client
 * @return {Promise}
 */
function getMongoClient() {
	if (mongoClient)
		return Promise.resolve(mongoClient);

	return new Promise((resolve, reject) => {
		const mongo = require('mongodb').MongoClient;
		mongo.connect(getMongoConnectionString(), (err, client) => {
			if (err) {
				console.log('Mongo connection error', err);
				return reject(err);
			}

			console.log('Mongo client connected');
			mongoClient = client;
			mongoDB = client.db(config.mongo.db);
			return resolve(mongoClient);
		});
	});
}

/**
 * Build mongo connection string
 * @return {string}
 */
function getMongoConnectionString() {
	return `mongodb://${encodeURIComponent(config.mongo.user)}:${encodeURIComponent(config.mongo.pass)}@${config.mongo.host}/${config.mongo.db}?${config.mongo.connectOptions}&replicaSet=${config.mongo.replicaSet}&ssl=true&${config.mongo.authOptions}`;
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

/**
 * Cycle through User's Drafts and process if target draft exist
 * @param {String} userId
 * @param {Object} resourceData
 */
function processDrafts(userId, resourceData) {
	let resourceObject = {};
	Object.keys(resourceData).forEach(propName => {
		if (!propName.startsWith('@') && !propName.startsWith('_')) {
			const newPropName = (propName === 'Id') ? 'item' + propName : propName;
			resourceObject[newPropName] = resourceData[propName];
		}
	});
	console.log('RESOURCE OBJECT TO UPDATE', resourceObject);
	mongoDB.collection('drafts').updateOne(
		{'user_id': userId, 'itemId': resourceData.Id},
		{$set: resourceObject},
		{upsert: true},
		function (err, res) {
			if (!!err) {
				console.log("DRAFT UPDATE ERROR", err);
			} else {
				console.log("DRAFT FOUND AND UPDATED");
			}
		}
	);
}

/**
 * Cycle through User's Drafts and process if target draft exist
 * @param {String} userId
 * @param {Object} resourceData
 */
function processDrafts(userId, resourceData) {
	let resourceObject = {};
	Object.keys(resourceData).forEach(propName => {
		if (!propName.startsWith('@') && !propName.startsWith('_')) {
			const newPropName = (propName === 'Id') ? 'item' + propName : propName;
			resourceObject[newPropName] = resourceData[propName];
		}
	});
	console.log('RESOURCE OBJECT TO UPDATE', resourceObject);
	mongoDB.collection('drafts').updateOne(
		{'user_id': userId, 'itemId': resourceData.Id},
		{$set: resourceObject},
		{upsert: false},
		function (err, res) {
			if (!!err) {
				console.log("DRAFT UPDATE ERROR", err);
			} else {
				console.log("DRAFT FOUND AND UPDATED");
			}
		}
	);
}
