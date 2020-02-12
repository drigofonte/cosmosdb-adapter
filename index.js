const { CosmosClient, ItemResponse, FeedResponse, PermissionMode } = require('@azure/cosmos');

module.exports.CosmosDb = class {
    /**
     * @param {string} uri 
     * @param {string} key 
     */
    constructor(uri, key) {
        this.client = new CosmosClient({ endpoint: uri, key: key});
    }

    /**
     * @param {string} databaseId 
     * @param {string} containerId 
     * @param {object} document 
     * @returns Promise<ItemResponse<object>>
     */
    async write(databaseId, containerId, document) {
        return await this.client
            .database(databaseId)
            .container(containerId)
            .items.create(document);
    }

    /**
     * @param {string} databaseId 
     * @param {string} containerId 
     * @param {string} id 
     * @param {string} partitionValue 
     * @param {object} document 
     * @returns Promise<ItemResponse<object>>
     */
    async replace(databaseId, containerId, id, partitionValue, document) {
        return await this.client
            .database(databaseId)
            .container(containerId)
            .item(id, partitionValue)
            .replace(document);
    }


    /**
     * @param {string} databaseId 
     * @param {string} containerId 
     * @param {string} idProperty 
     * @param {string} idValue 
     * @param {string} partitionProperty 
     * @param {string} partitionValue 
     * @returns Promise<FeedResponse<any>>
     */
    async get(databaseId, containerId, idProperty, idValue, partitionProperty, partitionValue) {
        let query = `SELECT * FROM ${containerId} as c WHERE c.${idProperty} = '${idValue}' and c.${partitionProperty} = '${partitionValue}'`;
        return await this.execute(databaseId, containerId, query);
    }

    /**
     * @param {string} databaseId 
     * @param {string} containerId 
     * @param {string} query 
     * @returns Promise<FeedResponse<any>>
     */
    async execute(databaseId, containerId, query) {
        const querySpec = {
            query: query,
            parameters: []
        };
        return await this.client
            .database(databaseId)
            .container(containerId)
            .items.query(querySpec)
            .fetchAll();
    }

    /**
     * @param {string} databaseId 
     * @param {string} containerId 
     * @param {string} itemId 
     * @param {string} partitionKey 
     * @param {string} userId 
     * @returns Promise<object>
     */
    async getItemReadToken(databaseId, containerId, itemId, partitionKey, userId) {
        const database = await this.client.database(databaseId);
        const container = await database.container(containerId);
        const item = await container.item(itemId, partitionKey);

        let user;
        try {
            const userResponse = await database.users.create({ id: userId });
            user = userResponse.user;
        } catch(err) {
            user = await database.user(userId);
        }

        const permissionDef = { id: `${databaseId}.${containerId}.${partitionKey}.${itemId}.read`, permissionMode: PermissionMode.Read, resource: item.url };
        let permission;
        try {
            const permissionResponse = await user.permissions.create(permissionDef, { resourceTokenExpirySeconds: 3600 });
            permission = permissionResponse.permission;
        } catch(err) {
            permission = await user.permission(permissionDef.id);
        }

        const { resource: permDef } = await permission.read();
        return { [item.url]: permDef._token };
    }
}