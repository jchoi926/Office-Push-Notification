'use strict';

// Return request parameters in body as text
exports.handler = (event, context, callback) => {
	let body = '';
	const numberOfParameters = Object.keys(event.queryStringParameters).length;

	if (numberOfParameters == 1)
		body = event.queryStringParameters[Object.keys(event.queryStringParameters)[0]]
	else if (numberOfParameters > 1)
		body = JSON.stringify(event.queryStringParameters)

	callback(null, {
		statusCode: 200,
		headers: {
			'Content-Type': 'text/plain'
		},
		body: body
	})

}
