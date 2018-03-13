const AWS = require('aws-sdk');
const Promise = require('bluebird');
const config = {};
let redisClient;
let mongoClient;

exports.handler = handleIt;

/**
 * Lambda endpoint handler
 */
function handleIt(event, context, callback) {
	const headers = event.headers;
	const requestBody = JSON.parse(event.body);
	console.log("EVENT", event);
	console.log("REQUEST-HEADERS", headers);
	console.log("REQUEST_BODY", requestBody);
	console.log("REQUEST_RESOURCE_DATA", requestBody.resourceData);

	const response = {
		statusCode: 200,
		headers: {
			'Content-Type': 'text/plain'
		},
		isBase64Encoded: false
	};
	let responseBody;

	// handle subscription validation
	if (event.queryStringParameters && Object.keys(event.queryStringParameters).indexOf('validationtoken') > -1) {
		responseBody = event.queryStringParameters[Object.keys(event.queryStringParameters)[0]];
		callback(null, response);
	}
	// notifications
	else {
		initialize(false)
			.then(() => {
				/*redisClient.hgetallAsync("user:0051a000000aX5PAAU")
					.then(user => {
						responseBody.user = user;
						responseBody = JSON.stringify(responseBody);
						response.body = responseBody;
						cleanUp();

						callback(null, response);
					})
				;
				callback(null, response);*/
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
				useMongo ? getMongoClient() : Promise.resolve()
			]);
		})
	;
}

/**
 * Clean up
 */
function cleanUp() {
	if (redisClient) redisClient.quit();
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
			mongoClient = client.db(config.mongoDb);
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
