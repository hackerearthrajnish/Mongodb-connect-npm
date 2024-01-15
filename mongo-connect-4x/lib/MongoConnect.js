//todo throw error in aggregate if group is null,also ensure same error in other part as well
const MongoClient = require("mongodb").MongoClient;

const { typeCastSchema, typeCastFilter, modifyFields } = require("./TypeCastFunctions");

module.exports = async ({ config }) => {
  const { host, port, dbName, authEnabled, user, pwd } = config;

  const connect = async () => {
    let url = `mongodb://${host}:${port}`;
    if (authEnabled) {
      url = `mongodb://${user}:${pwd}@${host}:${port}`;
    }
    // Connect using MongoClient
    const client = await MongoClient.connect(url);
    return client.db(dbName);
  };

  // Got the db connection
  let db = await connect();

  const insert = async (table, document, { options = {}, schema } = {}) => {
    try {
      document = typeCastSchema({ value: document, schema });
      const result = await db.collection(table).insertOne(document, options);
      return result;
    } catch (e) {
      throw e;
    }
  };

  const insertMany = async (table, documents, { options = {}, schema } = {}) => {
    try {
      insert = typeCastSchema({ value: insert, schema });
      const result = await db.collection(table).insertMany(documents, options);
      return result;
    } catch (e) {
      throw e;
    }
  };

  const remove = async (table, filter, { schema, options = {} } = {}) => {
    filter = typeCastFilter({ filter, schema });
    options = { w: 1, ...options };
    const result = await db.collection(table).removeOne(filter, options);
    return result;
  };

  const removeMany = async (table, filter, { schema, options = {} } = {}) => {
    filter = typeCastFilter({ filter, schema });
    options = { w: 1, ...options };
    const result = await db.collection(table).removeMany(filter, options);
    return result;
  };

  const upsert = async (table, filter, update, { schema, options = {} } = {}) => {
    filter = typeCastFilter({ filter, schema });
    options = { ...options, new: true, upsert: true };
    const result = await db.collection(table).updateOne(filter, update, options);
    return result;
  };

  const update = async (table, filter, update, { schema, options = {} } = {}) => {
    let { $set, ...rest } = update;
    if ($set) {
      $set = typeCastSchema({ value: $set, schema });
      update = { $set, ...rest };
    }
    filter = typeCastFilter({ filter, schema });
    options = { w: 1, ...options };
    const result = await db.collection(table).updateOne(filter, update, options);
    return result;
  };

  const updateMany = async (table, filter, update, { schema, options = {} } = {}) => {
    let { $set, ...rest } = update;
    if ($set) {
      $set = typeCastSchema({ value: $set, schema });
      update = { $set, ...rest };
    }
    filter = typeCastFilter({ filter, schema });
    options = { w: 1, ...options };
    const result = await db.collection(table).updateMany(filter, update, options);
    return result;
  };

  const find = async (table, query = {}, { options, schema } = {}) => {
    let { filter, fields, ...restOptions } = query;
    const modifiedFields = modifyFields(fields);
    filter = typeCastFilter({ filter, schema });
    const result = await db
      .collection(table)
      .find(filter, { fields: modifiedFields, ...restOptions })
      .toArray();
    return result;
  };

  const aggregate = async (table, pipeline, options) => {
    const result = await db.collection(table).aggregate(pipeline, { allowDiskUse: true });
    return result;
  };

  const getIndexes = async table => {
    const indexes = await mongoDB.collection(table).indexes();
    return indexes;
  };

  const dropIndex = async (table, indexName) => {
    const result = await db.collection(table).dropIndex(indexName);
    return result;
  };

  const createIndex = async (table, index, options) => {
    const result = await db.collection(table).createIndex(index, options);
    return result;
  };

  const dropDB = async () => {
    const result = await db.dropDatabase();
    return result;
  };

  const dropTable = async () => {
    const result = await db.dropCollection();
    return result;
  };

  return {
    insert,
    insertMany,
    remove,
    removeMany,
    upsert,
    update,
    updateMany,
    find,
    aggregate,
    getIndexes,
    createIndex,
    dropIndex,
    dropTable,
    dropDB
  };
};
