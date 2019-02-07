/*
 * This project based on https://github.com/awslabs/amazon-elasticsearch-lambda-samples
 * Sample code for AWS Lambda to get AWS ELB log files from S3, parse
 * and add them to an Amazon Elasticsearch Service domain.
 *
 *
 * Copyright 2019- Amazon.com, Inc. or its affiliates.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at http://aws.amazon.com/asl/
 * or in the "license" file accompanying this file.  This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * express or implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

/* Imports */
var AWS = require('aws-sdk');
var LineStream = require('byline').LineStream;
var parse = require('elb-log-parser'); // elb-log-parser  https://github.com/toshihirock/node-elb-log-parser
var path = require('path');
var stream = require('stream');
const zlib = require('zlib');

var indexTimestamp = new Date().toISOString().replace(/\-/g, '.').replace(/T.+/, '');

/* Globals */
var esDomain = {
	endpoint: 'search-futbolx-elasticsearch-aqhnjvimoy2f5koczu5jzq4qee.us-east-1.es.amazonaws.com',
	region: 'us-east-1',
	doctype: 'elb-access-logs'
};
var endpoint = new AWS.Endpoint(esDomain.endpoint);
var s3 = new AWS.S3();
var totLogLines = 0; // Total number of log lines in the file
var numDocsAdded = 0; // Number of log lines added to ES so far

var deleteOp = {};

/*
 * The AWS credentials are picked up from the environment.
 * They belong to the IAM role assigned to the Lambda function.
 * Since the ES requests are signed using these credentials,
 * make sure to apply a policy that permits ES domain operations
 * to the role.
 */
var creds = new AWS.EnvironmentCredentials('AWS');

function addFileToDelete(bucket, key) {
	if (deleteOp[bucket]) {
		deleteOp[bucket].Delete.Objects.push({
			Key: key
		});
	} else {
		deleteOp[bucket] = {
			Bucket: bucket,
			Delete: {
				Objects: [{
						Key: key
					}
				]
			}
		};
	}
}
/*
 * Get the log file from the given S3 bucket and key.  Parse it and add
 * each log record to the ES domain.
 */
function s3LogsToES(bucket, key, context, lineStream, recordStream) {
	// Note: The Lambda function should be configured to filter for .log files
	// (as part of the Event Source "suffix" setting).

	addFileToDelete(bucket, key);

	var s3Stream = s3.getObject({
			Bucket: bucket,
			Key: key
		}).createReadStream();
	var indexName = bucket + '-' + indexTimestamp;

	console.log("s3LogsToES", key);
	// Flow: S3 file stream -> Log Line stream -> Log Record stream -> ES
	s3Stream
	.pipe(zlib.createGunzip())
	.pipe(lineStream)
	.pipe(recordStream)
	.on('data', function (parsedEntry) {
		postDocumentToES(indexName, parsedEntry, context);
	});

	s3Stream.on('error', function () {
		console.log(
			'Error getting object "' + key + '" from bucket "' + bucket + '".  ' +
			'Make sure they exist and your bucket is in the same region as this function.');
		context.fail();
	});
}

function addCustomFields(doc) {
	try {
		if (typeof(doc) == 'string') {
			doc = JSON.parse(doc);
		}
		var webAppName = doc.request_uri_path.match("^\/([a-z0-9]+)/")[1];
		if (webAppName) {
			doc.request_uri_base = webAppName;
		}
	} catch (e) {
		console.log(typeof(doc), doc, e);
	}
	return JSON.stringify(doc);
}

/*
 * Add the given document to the ES domain.
 * If all records are successfully added, indicate success to lambda
 * (using the "context" parameter).
 */
function postDocumentToES(indexName, doc, context) {

	doc = addCustomFields(doc);
	var req = new AWS.HttpRequest(endpoint);

	req.method = 'POST';
	req.path = path.join('/', indexName, esDomain.doctype);
	req.region = esDomain.region;
	req.body = doc;
	req.headers['presigned-expires'] = false;
	req.headers['Host'] = endpoint.host;
	req.headers['Content-Type'] = 'application/json';

	var signer = new AWS.Signers.V4(req, 'es');
	signer.addAuthorization(creds, new Date());

	// Post document to ES
	var send = new AWS.NodeHttpClient();

	send.handleRequest(req, null, function (httpResp) {
		var body = '',
		status = httpResp.statusCode;
		httpResp.on('error', function (chunk) {
			console.log("ERROR:", doc, chunk);
		});
		httpResp.on('data', function (chunk) {
			body += chunk;
		});

		httpResp.on('end', function (chunk) {
			numDocsAdded++;
			if (numDocsAdded === totLogLines && status >= 200 && status <= 399) {
				// Mark lambda success.  If not done so, it will be retried.
				console.log('All ' + numDocsAdded + ' log records added to ES.');
				var keys = Object.keys(deleteOp);
				var deletedOK = 0;
				var deletedError = 0;
				keys.forEach(function (key) {
					var deleteParam = deleteOp[key];
					s3.deleteObjects(deleteParam, function (err, data) {
						if (err) {
							deletedError++;
							console.log("Error Deleting Documents", JSON.stringify(deleteParam), err, err.stack);
						} else {
							deletedOK++;
							console.log('Deleted all indexed documents', JSON.stringify(deleteParam), JSON.stringify(data));
						}
						if ((deletedOK + deletedError - keys.length) == 0) {
							if (deletedOK = keys.length)
								context.succeed();
							else
								context.fail();
						}
					});
				});
			}
		});
	}, function (err) {
		console.log('Error: ' + err);
		console.log(numDocsAdded + 'of ' + totLogLines + ' log records added to ES.');
		context.fail();
	});
}

/* Lambda "main": Execution starts here */
exports.handler = function (event, context) {
	console.log('(v1.03) Received event from S3: ', JSON.stringify(event, null, 2));

	/* == Streams ==
	 * To avoid loading an entire (typically large) log file into memory,
	 * this is implemented as a pipeline of filters, streaming log data
	 * from S3 to ES.
	 * Flow: S3 file stream -> Log Line stream -> Log Record stream -> ES
	 */
	var lineStream = new LineStream();
	// A stream of log records, from parsing each log line
	var recordStream = new stream.Transform({
			objectMode: true
		})
		recordStream._transform = function (line, encoding, done) {
		var logRecord = parse(line.toString());
		var serializedRecord = JSON.stringify(logRecord);
		this.push(serializedRecord);
		totLogLines++;
		done();
	}

	event.Records.forEach(function (record) {
		var bucket = record.s3.bucket.name;
		var objKey = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '));
		s3LogsToES(bucket, objKey, context, lineStream, recordStream);
	});
}
