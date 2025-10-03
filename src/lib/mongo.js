const { MongoClient } = require('mongodb');

async function connectMongo(uri, log) {
  const client = new MongoClient(uri, { ignoreUndefined: true });
  await client.connect();
  log?.info(`Connected to ${uri.replace(/:\\S+@/, '://***:***@')}`);
  return client;
}

async function openConnections(config, logger) {
  const log = logger.child('mongo');
  const connections = {};

  if (config.sourceUri === config.targetUri) {
    const client = await connectMongo(config.sourceUri, log);
    connections.sourceClient = client;
    connections.targetClient = client;
  } else {
    const [sourceClient, targetClient] = await Promise.all([
      connectMongo(config.sourceUri, log.child('source')),
      connectMongo(config.targetUri, log.child('target')),
    ]);
    connections.sourceClient = sourceClient;
    connections.targetClient = targetClient;
  }

  connections.sourceDb = connections.sourceClient.db(config.sourceDb);
  connections.targetDb = connections.targetClient.db(config.targetDb);

  return connections;
}

async function closeConnections(connections) {
  const closes = [];
  if (connections.sourceClient && connections.sourceClient !== connections.targetClient) {
    closes.push(connections.sourceClient.close());
  }
  if (connections.targetClient) {
    closes.push(connections.targetClient.close());
  }
  await Promise.allSettled(closes);
}

module.exports = {
  openConnections,
  closeConnections,
};
