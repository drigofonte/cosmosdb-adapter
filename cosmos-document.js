const { CosmosDbAdapter } = require('./cosmos-adapter');

class CosmosDocument {
  /**
   * @param {String} id 
   * @param {String} partitionId 
   * @param {String} container 
   * @param {String} url 
   * @param {String} key 
   * @param {String} db 
   * @param {Function} assignFunc 
   */
  constructor(id, partitionId, container, url, key, db, assignFunc) {
    this.id = id;
    this.partitionId = partitionId;
    this.container = container;
    this.url = url;
    this.key = key;
    this.db = db;
    this.assignFunc = assignFunc;
  }

  /**
   * To be overriden by extensions of this class.
   * 
   * @returns {Boolean} true if the write should go ahead, false otherwise
   */
  onBeforeWrite() { return true; }

  async write() {
    if (this.onBeforeWrite()) {
      const cosmosdb = new CosmosDbAdapter(this.url, this.key);
      const obj = { ...this };
      delete obj.partitionId;
      delete obj.container;
      delete obj.key;
      delete obj.url;
      delete obj.db;

      if (this.id === undefined) {
          await cosmosdb.write(this.db, this.container, obj);
      } else {
          await cosmosdb.replace(this.db, this.container, this.id, this.partitionId, obj);
      }
    }
    return this;
  }

  /**
   * @returns {{ data: Object, status: Number }}
   * @throws {{ message: String, status: Number }}
   */
  async load() {
    const cosmosdb = new CosmosDbAdapter(this.url, this.key);
    const { statusCode: status, resource } = await cosmosdb.read(this.db, this.container, this.id, this.partitionId);
    if (status < 300) {
      const obj = this.assignFunc(resource);
      for (const [ key, value ] of Object.entries(obj)) {
        this[key] = value;
      }
    } else {
      switch (status) {
        case 404:
          throw { message: `Could not find resource with partition/id: ${this.partitionId}/${this.id}`, status };
        default:
          throw { message: `Could not load resource with partition/id: ${this.partitionId}/${this.id}`, status };
      }
    }
    return this;
  }

  /**
   * @param {String} query 
   */
  async getSingle(query) {
    return await CosmosDocument.getSingle(query, this.container, this.assignFunc, this.url, this.key, this.db);
  }

  /**
   * @param {String} query 
   */
  async getAll(query) {
    return await CosmosDocument.getAll(query, this.container, this.assignFunc, this.url, this.key, this.db);
  }

  toJSON() {
    const obj = { ...this };
    delete obj.partitionId;
    delete obj.container;
    delete obj.key;
    delete obj.url;
    delete obj.db;
    return obj;
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

  static async getAll(query, container, assignFunc, url, key, db) {
    const cosmosdb = new CosmosDbAdapter(url, key);
    const response = await cosmosdb.execute(db, container, query);
    return response.resources.map(r => assignFunc(r));
  }
}

module.exports.CosmosDocument = CosmosDocument;