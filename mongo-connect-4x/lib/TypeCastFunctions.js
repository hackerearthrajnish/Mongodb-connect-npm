const ObjectId = require("mongodb").ObjectID;
const ObjConstructor = {}.constructor;

module.export = () => {
  const isJSONObject = obj => {
    if (
      obj === undefined ||
      obj === null ||
      obj === true ||
      obj === false ||
      typeof obj !== "object" ||
      Array.isArray(obj)
    ) {
      return false;
    } else if (obj.constructor === ObjConstructor || obj.constructor === undefined) {
      return true;
    } else {
      return false;
    }
  };

  const getObjectId = value => {
    if (value === undefined || value === null) {
      return value;
    }
    try {
      return new ObjectId(value);
    } catch (e) {
      return value;
    }
  };

  const typecastFilterValue = ({ value, schemaDef }) => {
    if (value === undefined) {
      throw new Error("Filter value can not be undefined");
    } else if (value === null) {
      return value;
    } else if (!schemaDef) {
      return value;
    } else if (Array.isArray(value)) {
      return value.map(v => typecastFilterValue({ value: v, schemaDef }));
    } else if (isJSONObject(value)) {
      let newValue = {};
      for (let opKey in value) {
        newValue[opKey] = typecastFilterValue({ value: value[opKey], schemaDef });
      }
      return newValue;
    } else if (schemaDef === "objectId") {
      return getObjectId(value);
    } else if (schemaDef === "date") {
      return new Date(value);
    } else {
      return value;
    }
  };

  const getFilterKeySchema = ({ field, schema }) => {
    const dotIndex = field.indexOf(".");
    if (dotIndex > 0) {
      let preField = field.substring(0, dotIndex);
      let fieldSchema = schema[preField];
      if (fieldSchema) {
        return getFilterKeySchema({ field: field.substring(dotIndex + 1), schema: fieldSchema.schema });
      } else {
        return void 0;
      }
    } else {
      return schema[field];
    }
  };

  const typeCastFilter = ({ filter, schema }) => {
    if (!schema || !Object.keys(schema) || !filter || !Object.keys(filter).length) {
      return filter;
    }

    let newFilter = undefined;
    if (Array.isArray(filter)) {
      newFilter = filter.map(innerFilter => typeCastFilter({ filter: innerFilter, schema }));
    } else {
      newFilter = {};
      for (let key in filter) {
        let value = filter[key];
        if (key === "$and" || key === "$or") {
          value = typeCastFilter({ filter: value, schema });
        } else {
          const schemaDef = getFilterKeySchema({ field: key, schema });
          if (schemaDef) {
            value = typecastFilterValue({ value, schemaDef });
          }
        }

        newFilter[key] = value;
      }
    }
    return newFilter;
  };

  const typeCastSchema = ({ value, schema, keep }) => {
    if (!schema || !Object.keys(schema).length) {
      return value;
    }
    let newValue = void 0;
    if (Array.isArray(value)) {
      newValue = value.map(v => typeCastSchema({ value: v, schema, keep }));
    } else {
      newValue = {};
      for (let key in value) {
        let keyValue = value[key];
        if (keep && !keep[key]) {
          continue;
        }
        const schemaDef = schema[key];
        if (schemaDef) {
          if (schemaDef === "objectId") {
            if (Array.isArray(keyValue)) {
              keyValue = keyValue.map(nestedDoc => getObjectId(nestedDoc));
            } else {
              keyValue = getObjectId(keyValue);
            }
          } else if (schemaDef === "date") {
            keyValue = new Date(keyValue);
          } else if (typeof schemaDef === "object") {
            keyValue = typeCastSchema({ value: keyValue, schema: schemaDef.schema, keep: schemaDef.keep });
          } else {
            // throw new Error(`Unsupported schema def for key: [${key}]. Schema def [${JSON.stringify(schemaDef)}]`);
          }
        }
        newValue[key] = keyValue;
      }
    }
    return newValue;
  };

  const modifyFields = fields => {
    const modifiedFields = {};

    const populateFields = (fields, modifiedFields, key) => {
      if (fields) {
        if (Array.isArray(fields)) {
          for (const field of fields) {
            const f = key ? `${key}.${field}` : field;
            if (typeof field === "string") {
              modifiedFields[f] = 1;
            } else if (typeof field === "object") {
              populateFields(field, modifiedFields, key);
            }
          }
        } else if (isJSONObject(fields)) {
          for (const field in fields) {
            const value = fields[field];
            const f = key ? `${key}.${field}` : field;
            if (value !== 1) {
              populateFields(value, modifiedFields, f);
            } else {
              modifiedFields[f] = 1;
            }
          }
        } else {
          throw new Error(`Invalid type of fields [${fields}]`);
        }
      }
    };
    if (fields && Object.keys(fields).length) {
      populateFields(fields, modifiedFields);
    }
    return modifiedFields;
  };

  return { typeCastFilter, typeCastSchema, modifyFields };
};
