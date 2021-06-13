const { CosmosDbAdapter: cosmosdb } = require('./cosmos-adapter-singleton');
const { ItemResponse } = require('@azure/cosmos');

class CosmosDocument {
  /**
   * @param {String} id 
   * @param {String} partitionId 
   * @param {String} container 
   * @param {String} db 
   * @param {Function} assignFunc 
   */
  constructor(id, partitionId, container, db, assignFunc) {
    this.id = id;
    this.partitionId = partitionId;
    this.container = container;
    this.db = db;
    this.assignFunc = assignFunc;
  }

  /**
   * To be overriden by extensions of this class.
   * 
   * @returns {Boolean} true if the write should go ahead, false otherwise
   */
  async onBeforeWrite() { return true; }

  /**
   * To be overriden by extensions of this class.
   * 
   * @returns {Boolean} true if the document creation should go ahead, false otherwise
   */
  async onBeforeCreate() { return true; }

  /**
   * To be overriden by extensions of this class.
   * 
   * @param {ItemResponse<any>} response
   */
  async onCreated(response) { }

  /**
   * To be overriden by extensions of this class.
   * 
   * @returns {Boolean} true if the document update should go ahead, false otherwise
   */
  async onBeforeUpdate() { return true; }

  /**
   * To be overriden by extensions of this class.
   * 
   * @param {ItemResponse<any>} response
   */
  async onUpdated(response) { }

  /**
   * @param {Boolean} isNew force the document to be written to the database as a new document
   */
  async write(isNew = false) {
    if (await this.onBeforeWrite()) {
      const obj = { ...this };
      delete obj.partitionId;
      delete obj.container;
      delete obj.key;
      delete obj.url;
      delete obj.db;

      if (this.id === undefined || isNew) {
        const cont = await this.onBeforeCreate();
        if (cont) {
          const res = await cosmosdb.write(this.db, this.container, obj);
          this.id = res.resource.id;
          await this.onCreated(res);
        }
      } else {
        const cont = await this.onBeforeUpdate();
        if (cont) {
          const res = await cosmosdb.replace(this.db, this.container, this.id, this.partitionId, obj);
          await this.onUpdated(res);
        }
      }
    }
    return this;
  }

  /**
   * @throws {{ message: String, status: Number }}
   */
  async load() {
    let resource;
    let status = 404;
    if (this.id && this.partitionId) {
      ({ status, resource } = await this.loadByPointRead());
    } else if (this.id) {
      ({ status, resource } = await this.loadById());
    }

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
   * @returns {{ status: String, resource: Object }}
   */
  async loadByPointRead() {
    const { statusCode: status, resource } = await cosmosdb.read(this.db, this.container, this.id, this.partitionId);
    return { status, resource };
  }

  /**
   * @returns {{ status: String, resource: Object }}
   */
  async loadById() {
    const query = `SELECT * FROM c WHERE c.id = '${this.id}'`;
    const { statusCode: status, resources } = await cosmosdb.execute(this.db, this.container, query);
    return { status, resource: resources[0] };
  }

  /**
   * @param {String} query 
   */
  async getSingle(query) {
    return await CosmosDocument.getSingle(query, this.container, this.assignFunc, this.db);
  }

  /**
   * @param {String} query 
   */
  async getAll(query) {
    return await CosmosDocument.getAll(query, this.container, this.assignFunc, this.db);
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

  static async getSingle(query, container, assignFunc, db) {
    const response = await cosmosdb.execute(db, container, query);
    let obj;
    if (response.resources.length > 0) {
        obj = assignFunc(response.resources[0]);
    }
    return obj;
  }

  static async getAll(query, container, assignFunc, db) {
    const response = await cosmosdb.execute(db, container, query);
    return response.resources.map(r => assignFunc(r));
  }
}

module.exports.CosmosDocument = CosmosDocument;