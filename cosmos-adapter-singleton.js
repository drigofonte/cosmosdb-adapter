const { CosmosDbAdapter } = require('./cosmos-adapter');

const { 
  $COSMOSDB_ENDPOINT: endpoint, 
  $COSMOSDB_KEY: key,
  $COSMOSDB_ENABLE_ENDPOINT_DISCOVERY: endpointDiscovery
} = process.env || {};

/** @type {CosmosDbAdapter} */
let adapter;
if (!endpoint) {
  throw new Error('$COSMOSDB_ENDPOINT environment variable not defined. Failed to initialise CosmosDB adapter.');
} else if (!key) {
  throw new Error('$COSMOSDB_KEY environment variable not defined. Failed to initialise CosmosDB adapter.');
} else {
  const enableEndpointDiscovery = endpointDiscovery === 'true' 
    || endpointDiscovery === undefined;
  adapter = new CosmosDbAdapter(endpoint, key, { enableEndpointDiscovery });
}

module.exports.CosmosDbAdapter = adapter;