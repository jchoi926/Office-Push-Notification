const AWS = require('aws-sdk');
const Promise = require('bluebird');
const config = {};
let redisClient;
let mongoClient;
let mongoDb;

exports.handler = handleIt;

/**
 * Lambda endpoint handler
 */
function handleIt(event, context, callback) {
	console.log("EVENT", event);

	const response = {
		statusCode: 200,
		/*headers: {
			'Content-Type': 'text/plain'
		},*/
		isBase64Encoded: false
	};
	let responseBody;

	// handle subscription validation
	if (event.queryStringParameters && Object.keys(event.queryStringParameters).indexOf('validationtoken') > -1) {
		responseBody = event.queryStringParameters[Object.keys(event.queryStringParameters)[0]];
		response.body = responseBody;
		callback(null, response);
	}
	// notifications
	else {
		const headers = event.headers;
		const requestBody = JSON.parse(event.body);
		console.log("REQUEST-HEADERS", headers);
		console.log("REQUEST_BODY", requestBody);
		console.log("REQUEST_RESOURCE_DATA", requestBody.resourceData);

		initialize()
			.then(() => {
				console.log('Lambda initialized');
				return redisClient.hgetallAsync("user:0051a000000aX5PAAU")
					.then(user => {
						responseBody = {
							user: user.user_id
						};
						response.body = JSON.stringify(responseBody);

						cleanUp();
						callback(null, response);
					})
				;
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
function initialize(useMongo) {
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
	if (mongoClient) mongoClient.close();
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
			mongoDB = client.db(config.mongoDb);
			return resolve(mongoClient);
		});
	});
}

/**
 * Build mongo connection string
 * @return {string}
 */
function getMongoConnectionString() {
	return `mongodb://${encodeURIComponent(config.mongoUser)}:${encodeURIComponent(config.mongoPass)}@${config.mongoHost}/${config.mongoDb}?${config.mongoConnectOptions}&replicaSet=${config.mongoReplicaSet}&ssl=true&${config.mongoAuthOptions}`;
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
		redisClient = redis.createClient({host: config.redisHost, port: parseInt(config.redisPort), password: config.redisPass});

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

	const SSM = Promise.promisifyAll(new AWS.SSM());
	return SSM.getParametersAsync({
		Names: [
			'redisHost',
			'redisPort',
			'redisPass',
			'mongoHost',
			'mongoUser',
			'mongoPass',
			'mongoDb',
			'mongoReplicaSet',
			'mongoConnectOptions',
			'mongoAuthOptions'
		],
		WithDecryption: true
	})
	.then(params => {
		params.Parameters.forEach(param => {
			config[param.Name] = param.Value;
		});
		return config;
	});
}
