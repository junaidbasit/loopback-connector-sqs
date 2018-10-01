

const _ = require('lodash');
const Promise = require('bluebird');
const AWS = require('aws-sdk');
const debug = require('debug')('loopback:connector:sqs');

const Queue = function () {};

Queue.create = Queue.send = function (message, options, cb) {
  if (_.isFunction(options) && cb === undefined) {
    cb = options;
    options = undefined;
  }

  const connector = this.dataSource.connector;
  const driver = connector.driver;
  const endpointSetting = {
    QueueUrl: connector.endpoint,
  };
  const sendMessage = Promise.promisify(driver.sendMessage, {
    context: driver,
  });

  (options || (options = {})).MessageBody = _.isObject(message) ? JSON.stringify(message) : message;
  debug('Calling SQS `sendMessage` with settings: %O', options);
  const promise = sendMessage(_.defaults(options, endpointSetting));

  if (_.isFunction(cb)) {
    promise.asCallback(cb);
  } else {
    return promise;
  }
};

Queue.prototype.save = function (options, cb) {
  return this.constructor.create(this, options, cb);
};

Queue.find = function (options, cb) {
  const connector = this.dataSource.connector;
  const driver = connector.driver;
  const endpointSetting = {
    QueueUrl: connector.endpoint,
  };
  const receiveMessage = Promise.promisify(driver.receiveMessage, {
    context: driver,
  });

  debug('Calling SQS `receiveMessage` with settings: %O', options);
  const promise = receiveMessage(_.defaults(options, endpointSetting));

  if (_.isFunction(cb)) {
    promise.asCallback(cb);
  } else {
    return promise;
  }
};

Queue.destroyByReceiptHandle = function (handle, cb) {
  const connector = this.dataSource.connector;
  const driver = connector.driver;
  const endpointSetting = {
    QueueUrl: connector.endpoint,
  };
  const options = {
    ReceiptHandle: handle,
  };
  const deleteMessage = Promise.promisify(driver.deleteMessage, {
    context: driver,
  });

  debug('Calling SQS `deleteMessage` with settings: %O', options);
  const promise = deleteMessage(_.defaults(options, endpointSetting));

  if (_.isFunction(cb)) {
    promise.asCallback(cb);
  } else {
    return promise;
  }
};

Queue.prototype.destroy = function (cb) {
  return this.constructor.destroyByReceiptHandle(this.ReceiptHandle, cb);
};

function SqsConnector(settings) {
  const constructorOptions = [
    'params',
    'endpoint',
    'accessKeyId',
    'secretAccessKey',
    'sessionToken',
    'credentials',
    'credentialProvider',
    'region',
    'maxRetries',
    'maxRedirects',
    'sslEnabled',
    'paramValidation',
    'min',
    'max',
    'pattern',
    'enum',
    'computeChecksums',
    'convertResponseTypes',
    'correctClockSkew',
    's3ForcePathStyle',
    's3BucketEndpoint',
    's3DisableBodySigning',
    'retryDelayOptions',
    'base',
    'customBackoff',
    'httpOptions',
    'proxy',
    'agent',
    'timeout',
    'xhrAsync',
    'xhrWithCredentials',
    'apiVersion',
    'apiVersions',
    'logger',
    'systemClockOffset',
    'signatureVersion',
    'signatureCache'];
  const connectorSettings = _.pick(settings, constructorOptions);

  this.driver = new AWS.SQS(connectorSettings);
  debug('Initialize SQS connector with settings: %O', connectorSettings);
  this.endpoint = settings.endpoint;
  debug('Default endpoint, if not provided: %s', this.endpoint);
}

SqsConnector.initialize = function (dataSource, cb) {
  dataSource.connector = new SqsConnector(dataSource.settings);

  if (_.isFunction(cb)) {
    cb();
  }
};

SqsConnector.prototype.DataAccessObject = Queue;

module.exports = SqsConnector;
