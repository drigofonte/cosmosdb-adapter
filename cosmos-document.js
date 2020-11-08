const { CosmosDbAdapter } = require('./cosmos-adapter');

class CosmosDocument {
    constructor(id, partitionId, container, url, key, db) {
        this.id = id;
        this.partitionId = partitionId;
        this.container = container;
        this.url = url;
        this.key = key;
        this.db = db;
    }

    /**
     * To be overriden by extensions of this class.
     * 
     * @returns {Boolean} true if the write should go ahead, false otherwise
     */
    onBeforeWrite() { return true; }

    /**
     * To be overriden by extensions of this class.
     * 
     * @returns {Function}
     */
    assignFunc() { return () => {} }

    async write() {
        if (this.onBeforeWrite()) {
            const cosmosdb = new CosmosDbAdapter(this.url, this.key);
            const obj = { ...this };
            delete obj.partitionId;
            delete obj.container;

            if (this.id === undefined) {
                await cosmosdb.write(this.db, this.container, obj);
            } else {
                await cosmosdb.replace(this.db, this.container, this.id, this.partitionId, obj);
            }
        }
        return this;
    }

    async getSingle(query) {
        return await CosmosDocument.getSingle(query, this.container, this.assignFunc, this.url, this.key, this.db);
    }

    static async getSingle(query, container, assignFunc, url, key, db) {
        const cosmosdb = new CosmosDbAdapter(url, key);
        const response = await cosmosdb.execute(db, container, query);
        let obj;
        if (response.resources.length > 0) {
            obj = assignFunc(response.resources[0]);
        }
        return obj;
    }
}

module.exports.CosmosDocument = CosmosDocument;