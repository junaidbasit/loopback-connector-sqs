

const _ = require('lodash');
const Promise = require('bluebird');
const AWS = require('aws-sdk');
const debug = require('debug')('loopback:connector:sqs');

const util = require('util');
const Connector = require('loopback-connector').Connector;

const Queue = function (settings, dataSource) {};


function SqsConnector(dataSource, settings) {
  Connector.call(this, 'sqs', settings);
  this.dataSource = dataSource;
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
    'signatureCache',
  ];
  const connectorSettings = _.pick(settings, constructorOptions);

  this.driver = new AWS.SQS(connectorSettings);
  debug('Initialize SQS connector with settings: %O', connectorSettings);
  this.endpoint = settings.endpoint;
  debug('Default endpoint, if not provided: %s', this.endpoint);
}

util.inherits(Queue, Connector);

SqsConnector.prototype.connect = function (cb) {
  // console.log('Queue.prototypeQueue.prototypeconnectconnectconnect');
  cb();
};
SqsConnector.prototype.create = SqsConnector.prototype.send = function () {
  // console.log('in sqs', '$$$$$$$$$$$$', arguments);
  let [message, options, cb] = [
    typeof arguments[0] === 'string' ? arguments[1] : arguments[0],
    typeof arguments[0] === 'string' ? {} : arguments[1],
    arguments[2],
  ];
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

  const paramKeys = [
    'QueueUrl',
    'MessageBody',
    'DelaySeconds',
    'MessageAttributes',
    'MessageDeduplicationId',
    'MessageGroupId',
  ];
  // console.log('options', options, '=========', message, '=========', _.isObject(message));

  (options || (options = {})).MessageBody = _.isObject(message) ? JSON.stringify(message) : message;
  for (key in options) if (paramKeys.indexOf(key) == -1) delete options[key];

  // console.log('options', options);
  debug('Calling SQS `sendMessage` with settings: %O', options);
  const promise = sendMessage(_.defaults(options, endpointSetting));

  if (_.isFunction(cb)) {
    promise.asCallback(cb);
  } else {
    return promise;
  }
};

SqsConnector.prototype.save = function save(options, cb) {
  // console.log('%%%%% save');
  return this.create.call(this, options, options, cb);
};

SqsConnector.prototype.patchOrCreate = function patchOrCreate(message, options, cb) {
  // console.log('%%%%% patchOrCreate');
  return this.create.call(this, message, options, cb);
};

SqsConnector.prototype.patchAttributes = function patchAttributes(options, cb) {
  // console.log('%%%%% patchAttributes');
  return this.create.call(this, message, options, cb);
};

SqsConnector.prototype.replaceOrCreate = function replaceOrCreate(message, options, cb) {
  // console.log('%%%%% replaceOrCreate', arguments);
  return this.create.call(this, message, options, cb);
};

SqsConnector.prototype.updateAttributes = function updateAttributes(id, receiveMessage, cb) {
  // console.log('%%%%% updateAttributes');
  receiveMessage.id = id;
  return this.create.call(this, receiveMessage, {}, cb);
};

SqsConnector.find = function (options, nutify, cb) {
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

SqsConnector.destroyByReceiptHandle = function (handle, cb) {
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

SqsConnector.prototype.destroy = function (cb) {
  return this.constructor.destroyByReceiptHandle(this.ReceiptHandle, cb);
};

SqsConnector.prototype.connect = function (cb) {
  // console.log('connect connect');
  cb();
};

SqsConnector.prototype.disconnect = function (cb) {
  // console.log('Queue.prototype.disconnect');
  if (cb) {
    process.nextTick(cb);
  }
};

SqsConnector.prototype.ping = function (cb) {
  // console.log('Queue.prototype.ping');
  if (cb) {
    process.nextTick(cb);
  }
};

SqsConnector.initialize = function (dataSource, cb) {
  dataSource.connector = new SqsConnector(dataSource, dataSource.settings);

  if (_.isFunction(cb)) {
    dataSource.connector.connect(cb);
  }
};

SqsConnector.prototype.DataAccessObject = SqsConnector.prototype;

module.exports = SqsConnector;
