import { buildError } from './apollo_error';
import { Schemer } from '../validators/schema';
import { Normalizer } from '../utils/normalizer';
import { Parser } from '../utils/parser';
import { ElassandraBuilder } from '../builders/elassandra';
import { JanusGraphBuilder } from '../builders/janusgraph';
import { TableBuilder } from '../builders/table';
import { Driver } from '../helpers/driver';

const Promise = require('bluebird');
const _ = require('lodash');
const util = require('util');

let dseDriver;
try {
  // eslint-disable-next-line import/no-extraneous-dependencies, import/no-unresolved
  dseDriver = require('dse-driver');
} catch (e) {
  dseDriver = null;
}

const cql = Promise.promisifyAll(dseDriver || require('cassandra-driver'));

export class BaseModel {
  private _modified: {};
  private _validators: {};

  private static _properties = {
    name: null,
    schema: null,
  };
  private static _driver: Driver;
  private static _ready: boolean;

  private static _modified = {};

  constructor(instanceValues) {
    instanceValues = instanceValues || {};
    const fieldValues = {};
    const fields = BaseModel._properties.schema.fields;
    const methods = BaseModel._properties.schema.methods || {};
    // const model = BaseModel;

    const defaultSetter = function f1(propName, newValue) {
      if (this[propName] !== newValue) {
        BaseModel._modified[propName] = true;
      }
      this[propName] = newValue;
    };

    const defaultGetter = function f1(propName) {
      return this[propName];
    };

    this._modified = {};
    this._validators = {};

    for (
      let fieldsKeys = Object.keys(fields), i = 0, len = fieldsKeys.length;
      i < len;
      i++
    ) {
      const propertyName = fieldsKeys[i];
      const field = fields[fieldsKeys[i]];

      try {
        this._validators[propertyName] = Schemer.getValidators(
          BaseModel._properties.schema,
          propertyName,
        );
      } catch (e) {
        throw buildError('model.validator.invalidschema', e.message);
      }

      let setter = defaultSetter.bind(fieldValues, propertyName);
      let getter = defaultGetter.bind(fieldValues, propertyName);

      if (field.virtual && typeof field.virtual.set === 'function') {
        setter = field.virtual.set.bind(fieldValues);
      }

      if (field.virtual && typeof field.virtual.get === 'function') {
        getter = field.virtual.get.bind(fieldValues);
      }

      const descriptor = {
        enumerable: true,
        set: setter,
        get: getter,
      };

      Object.defineProperty(this, propertyName, descriptor);
      if (
        field.virtual &&
        typeof instanceValues[propertyName] !== 'undefined'
      ) {
        this[propertyName] = instanceValues[propertyName];
      }
    }

    for (
      let fieldsKeys = Object.keys(fields), i = 0, len = fieldsKeys.length;
      i < len;
      i++
    ) {
      const propertyName = fieldsKeys[i];
      const field = fields[fieldsKeys[i]];

      if (
        !field.virtual &&
        typeof instanceValues[propertyName] !== 'undefined'
      ) {
        this[propertyName] = instanceValues[propertyName];
      }
    }

    for (
      let methodNames = Object.keys(methods), i = 0, len = methodNames.length;
      i < len;
      i++
    ) {
      const methodName = methodNames[i];
      this[methodName] = methods[methodName];
    }
  }

  private static _setProperties(properties) {
    const schema = properties.schema;
    const tableName = schema.table_name || properties.name;

    if (!Schemer.validateTableName(tableName)) {
      throw buildError('model.tablecreation.invalidname', tableName);
    }

    const qualifiedTableName = util.format(
      '"%s"."%s"',
      properties.keyspace,
      tableName,
    );

    this._properties = properties;
    this._properties.table_name = tableName;
    this._properties.qualified_table_name = qualifiedTableName;
    this._driver = new Driver(this._properties);
  }

  private static _syncModelDefinition(callback) {
    const properties = this._properties;
    const tableName = properties.table_name;
    const modelSchema = properties.schema;
    let migration = properties.migration;

    const tableBuilder = new TableBuilder(this._driver, this._properties);

    // backwards compatible change, dropTableOnSchemaChange will work like migration: 'drop'
    if (!migration) {
      if (properties.dropTableOnSchemaChange) migration = 'drop';
      else migration = 'safe';
    }
    // always safe migrate if NODE_ENV==='production'
    if (process.env.NODE_ENV === 'production') migration = 'safe';

    // check for existence of table on DB and if it matches this model's schema
    tableBuilder.getTableSchema((err, dbSchema) => {
      if (err) {
        callback(err);
        return;
      }

      const afterDBCreate = (err1) => {
        if (err1) {
          callback(err1);
          return;
        }

        const indexingTasks = [];

        // cassandra index create if defined
        if (_.isArray(modelSchema.indexes)) {
          tableBuilder.createIndexesAsync = Promise.promisify(
            tableBuilder.createIndexes,
          );
          indexingTasks.push(
            tableBuilder.createIndexesAsync(modelSchema.indexes),
          );
        }
        // cassandra custom index create if defined
        if (_.isArray(modelSchema.custom_indexes)) {
          tableBuilder.createCustomIndexesAsync = Promise.promisify(
            tableBuilder.createCustomIndexes,
          );
          indexingTasks.push(
            tableBuilder.createCustomIndexesAsync(modelSchema.custom_indexes),
          );
        }
        if (modelSchema.custom_index) {
          tableBuilder.createCustomIndexAsync = Promise.promisify(
            tableBuilder.createCustomIndexes,
          );
          indexingTasks.push(
            tableBuilder.createCustomIndexAsync([modelSchema.custom_index]),
          );
        }
        // materialized view create if defined
        if (modelSchema.materialized_views) {
          tableBuilder.createViewsAsync = Promise.promisify(
            tableBuilder.createMviews,
          );
          indexingTasks.push(
            tableBuilder.createViewsAsync(modelSchema.materialized_views),
          );
        }

        Promise.all(indexingTasks)
          .then(() => {
            // db schema was updated, so callback with true
            callback(null, true);
          })
          .catch((err2) => {
            callback(err2);
          });
      };

      if (!dbSchema) {
        if (properties.createTable === false) {
          callback(buildError('model.tablecreation.schemanotfound', tableName));
          return;
        }
        // if not existing, it's created
        tableBuilder.createTable(modelSchema, afterDBCreate);
        return;
      }

      let normalizedModelSchema;
      let normalizedDBSchema;

      try {
        normalizedModelSchema = Normalizer.normalizeModelSchema(modelSchema);
        normalizedDBSchema = Normalizer.normalizeModelSchema(dbSchema);
      } catch (e) {
        throw buildError('model.validator.invalidschema', e.message);
      }

      if (_.isEqual(normalizedModelSchema, normalizedDBSchema)) {
        // no change in db was made, so callback with false
        callback(null, false);
        return;
      }

      if (migration === 'alter') {
        // check if table can be altered to match schema
        if (
          _.isEqual(normalizedModelSchema.key, normalizedDBSchema.key) &&
          _.isEqual(
            normalizedModelSchema.clustering_order,
            normalizedDBSchema.clustering_order,
          )
        ) {
          tableBuilder.initAlterOperations(
            modelSchema,
            dbSchema,
            normalizedModelSchema,
            normalizedDBSchema,
            (err1) => {
              if (err1 && err1.message === 'alter_impossible') {
                tableBuilder.dropRecreateTable(
                  modelSchema,
                  normalizedDBSchema.materialized_views,
                  afterDBCreate,
                );
                return;
              }
              callback(err1);
            },
          );
        } else {
          tableBuilder.dropRecreateTable(
            modelSchema,
            normalizedDBSchema.materialized_views,
            afterDBCreate,
          );
        }
      } else if (migration === 'drop') {
        tableBuilder.dropRecreateTable(
          modelSchema,
          normalizedDBSchema.materialized_views,
          afterDBCreate,
        );
      } else {
        callback(
          buildError(
            'model.tablecreation.schemamismatch',
            tableName,
            'migration suspended, please apply the change manually',
          ),
        );
      }
    });
  }

  private static _syncEsIndex(callback) {
    const properties = this._properties;

    if (properties.esclient && properties.schema.es_index_mapping) {
      const keyspaceName = properties.keyspace;
      const mappingName = properties.table_name;
      const indexName = `${keyspaceName}_${mappingName}`;

      const elassandraBuilder = new ElassandraBuilder(properties.esclient);
      elassandraBuilder.assertIndex(keyspaceName, indexName, (err) => {
        if (err) {
          callback(err);
          return;
        }
        elassandraBuilder.putMapping(
          indexName,
          mappingName,
          properties.schema.es_index_mapping,
          callback,
        );
      });
      return;
    }
    callback();
  }

  private static _syncGraph(callback) {
    const properties = this._properties;

    if (properties.gremlin_client && properties.schema.graph_mapping) {
      const graphName = `${properties.keyspace}_graph`;
      const mappingName = properties.table_name;

      const graphBuilder = new JanusGraphBuilder(properties.gremlin_client);
      graphBuilder.assertGraph(graphName, (err) => {
        if (err) {
          callback(err);
          return;
        }
        graphBuilder.putMapping(
          graphName,
          mappingName,
          properties.schema.graph_mapping,
          callback,
        );
      });
      return;
    }
    callback();
  }

  private static _executeTableQuery(query, params, options, callback) {
    if (arguments.length === 3) {
      callback = options;
      options = {};
    }

    const defaults = {
      prepare: true,
    };

    options = _.defaultsDeep(options, defaults);

    const doExecuteQuery = function f1(doquery, docallback) {
      this.execute_query(doquery, params, options, docallback);
    }.bind(this, query);

    if (this.isTableReady()) {
      doExecuteQuery(callback);
    } else {
      this.init((err) => {
        if (err) {
          callback(err);
          return;
        }
        doExecuteQuery(callback);
      });
    }
  }

  private static _getFindQuery(queryObject, options) {
    const orderByClause = Parser.getOrderByClause(queryObject);
    const limitClause = Parser.getLimitClause(queryObject);
    const whereClause = Parser.getWhereClause(
      this._properties.schema,
      queryObject,
    );
    const selectClause = Parser.getSelectClause(options);
    const groupByClause = Parser.getGroupByClause(options);

    let query = util.format(
      'SELECT %s%s FROM "%s"',
      options.distinct ? 'DISTINCT ' : '',
      selectClause,
      options.materialized_view
        ? options.materialized_view
        : this._properties.table_name,
    );

    if (whereClause.query) query += util.format(' %s', whereClause.query);
    if (orderByClause) query += util.format(' %s', orderByClause);
    if (groupByClause) query += util.format(' %s', groupByClause);
    if (limitClause) query += util.format(' %s', limitClause);
    if (options.allow_filtering) query += ' ALLOW FILTERING';

    query += ';';

    return { query, params: whereClause.params };
  }

  static getTableName() {
    return this._properties.tableName;
  }

  static getKeyspaceName() {
    return this._properties.keyspace;
  }

  static isTableReady() {
    return this._ready === true;
  }

  static init(options, callback) {
    if (!callback) {
      callback = options;
      options = undefined;
    }

    this._ready = true;
    callback();
  }

  static syncDB(callback) {
    this._syncModelDefinition((err, result) => {
      if (err) {
        callback(err);
        return;
      }

      this._syncEsIndex((err1) => {
        if (err1) {
          callback(err1);
          return;
        }

        this._syncGraph((err2) => {
          if (err2) {
            callback(err2);
            return;
          }

          this._ready = true;
          callback(null, result);
        });
      });
    });
  }

  static getCqlClient(callback) {
    this._driver.ensureInit((err) => {
      if (err) {
        callback(err);
        return;
      }
      callback(null, this._properties.cql);
    });
  }

  static getEsClient() {
    if (!this._properties.esclient) {
      throw new Error(
        'To use elassandra features, set `manageESIndex` to true in ormOptions',
      );
    }
    return this._properties.esclient;
  }

  static getGremlinClient() {
    if (!this._properties.gremlin_client) {
      throw new Error(
        'To use janus graph features, set `manageGraphs` to true in ormOptions',
      );
    }
    return this._properties.gremlin_client;
  }

  static executeQuery(...args) {
    this._driver.executeQuery(...args);
  }

  static executeBatch(...args) {
    this._driver.executeBatch(...args);
  }

  static executeEachRow(...args) {
    this._driver.execute_eachRow(...args);
  }

  private static _executeTableEachRow(
    query,
    params,
    options,
    onReadable,
    callback,
  ) {
    if (this.isTableReady()) {
      this.executeEachRow(query, params, options, onReadable, callback);
    } else {
      this.init((err) => {
        if (err) {
          callback(err);
          return;
        }
        this.executeEachRow(query, params, options, onReadable, callback);
      });
    }
  }

  static eachRow(queryObject, options, onReadable, callback) {
    if (arguments.length === 3) {
      const cb = onReadable;
      onReadable = options;
      callback = cb;
      options = {};
    }
    if (typeof onReadable !== 'function') {
      throw buildError(
        'model.find.eachrowerror',
        'no valid onReadable function was provided',
      );
    }
    if (typeof callback !== 'function') {
      throw buildError('model.find.cberror');
    }

    const defaults = {
      raw: false,
      prepare: true,
    };

    options = _.defaultsDeep(options, defaults);

    options.return_query = true;
    const selectQuery = this.find(queryObject, options);

    const queryOptions = Normalizer.normalizeQueryOption(options);

    this._executeTableEachRow(
      selectQuery.query,
      selectQuery.params,
      queryOptions,
      (n, row) => {
        if (!options.raw) {
          const ModelConstructor = this._properties.get_constructor();
          row = new ModelConstructor(row);
          row._modified = {};
        }
        onReadable(n, row);
      },
      (err, result) => {
        if (err) {
          callback(buildError('model.find.dberror', err));
          return;
        }
        callback(err, result);
      },
    );
  }

  static execute_stream(...args) {
    this._driver.execute_stream(...args);
  }

  private static _executeTableStream(
    query,
    params,
    options,
    onReadable,
    callback,
  ) {
    if (this.isTableReady()) {
      this.execute_stream(query, params, options, onReadable, callback);
    } else {
      this.init((err) => {
        if (err) {
          callback(err);
          return;
        }
        this.executeStream(query, params, options, onReadable, callback);
      });
    }
  }

  static stream(queryObject, options, onReadable, callback) {
    if (arguments.length === 3) {
      const cb = onReadable;
      onReadable = options;
      callback = cb;
      options = {};
    }

    if (typeof onReadable !== 'function') {
      throw buildError(
        'model.find.streamerror',
        'no valid onReadable function was provided',
      );
    }
    if (typeof callback !== 'function') {
      throw buildError('model.find.cberror');
    }

    const defaults = {
      raw: false,
      prepare: true,
    };

    options = _.defaultsDeep(options, defaults);

    options.return_query = true;
    const selectQuery = this.find(queryObject, options);

    const queryOptions = Normalizer.normalizeQueryOption(options);

    const self = this;

    this._executeTableStream(
      selectQuery.query,
      selectQuery.params,
      queryOptions,
      function f1() {
        const reader = this;
        reader.readRow = () => {
          const row = reader.read();
          if (!row) return row;
          if (!options.raw) {
            const ModelConstructor = self._properties.get_constructor();
            const o = new ModelConstructor(row);
            o._modified = {};
            return o;
          }
          return row;
        };
        onReadable(reader);
      },
      (err) => {
        if (err) {
          callback(buildError('model.find.dberror', err));
          return;
        }
        callback();
      },
    );
  }

  private static _execute_gremlin_query(script, bindings, callback) {
    const gremlinClient = this.get_gremlin_client();
    gremlinClient.execute(script, bindings, (err, results) => {
      if (err) {
        callback(err);
        return;
      }
      callback(null, results);
    });
  }

  private static _execute_gremlin_script(script, bindings, callback) {
    this._execute_gremlin_query(script, bindings, (err, results) => {
      if (err) {
        callback(err);
        return;
      }
      callback(null, results[0]);
    });
  }

  static createVertex(vertexProperties, callback) {
    const properties = this._properties;
    const __graphName = `${properties.keyspace}_graph`;
    const __vertexLabel = properties.table_name;
    let script = `
    graph = ConfiguredGraphFactory.open(__graphName);
    vertex = graph.addVertex(__vertexLabel);
  `;
    Object.keys(vertexProperties).forEach((property) => {
      script += `vertex.property('${property}', ${property});`;
    });
    script += 'vertex';
    const bindings = _.defaults(vertexProperties, {
      __graphName,
      __vertexLabel,
    });
    this._execute_gremlin_script(script, bindings, callback);
  }

  static getVertex(__vertexId, callback) {
    const properties = this._properties;
    const __graphName = `${properties.keyspace}_graph`;
    const script = `
    graph = ConfiguredGraphFactory.open(__graphName);
    g = graph.traversal();
    vertex = g.V(__vertexId);
  `;
    const bindings = {
      __graphName,
      __vertexId,
    };
    this._execute_gremlin_script(script, bindings, callback);
  }

  static updateVertex(__vertexId, vertexProperties, callback) {
    const properties = this._properties;
    const __graphName = `${properties.keyspace}_graph`;
    let script = `
    graph = ConfiguredGraphFactory.open(__graphName);
    g = graph.traversal();
    vertex = g.V(__vertexId);
  `;
    Object.keys(vertexProperties).forEach((property) => {
      script += `vertex.property('${property}', ${property});`;
    });
    script += 'vertex';
    const bindings = _.defaults(vertexProperties, {
      __graphName,
      __vertexId,
    });
    this._execute_gremlin_script(script, bindings, callback);
  }

  static deleteVertex(__vertexId, callback) {
    const properties = this._properties;
    const __graphName = `${properties.keyspace}_graph`;
    const script = `
    graph = ConfiguredGraphFactory.open(__graphName);
    g = graph.traversal();
    vertex = g.V(__vertexId);
    vertex.drop();
  `;
    const bindings = {
      __graphName,
      __vertexId,
    };
    this._execute_gremlin_script(script, bindings, callback);
  }

  static createEdge(
    __edgeLabel,
    __fromVertexId,
    __toVertexId,
    edgeProperties,
    callback,
  ) {
    if (arguments.length === 4 && typeof edgeProperties === 'function') {
      callback = edgeProperties;
      edgeProperties = {};
    }
    const properties = this._properties;
    const __graphName = `${properties.keyspace}_graph`;
    let script = `
    graph = ConfiguredGraphFactory.open(__graphName);
    g = graph.traversal();
    fromVertex = g.V(__fromVertexId).next();
    toVertex = g.V(__toVertexId).next();
    edge = fromVertex.addEdge(__edgeLabel, toVertex);
  `;
    Object.keys(edgeProperties).forEach((property) => {
      script += `edge.property('${property}', ${property});`;
    });
    script += 'edge';
    const bindings = _.defaults(edgeProperties, {
      __graphName,
      __fromVertexId,
      __toVertexId,
      __edgeLabel,
    });
    this._execute_gremlin_script(script, bindings, callback);
  }

  static getEdge(__edgeId, callback) {
    const properties = this._properties;
    const __graphName = `${properties.keyspace}_graph`;
    const script = `
    graph = ConfiguredGraphFactory.open(__graphName);
    g = graph.traversal();
    edge = g.E(__edgeId);
  `;
    const bindings = {
      __graphName,
      __edgeId,
    };
    this._execute_gremlin_script(script, bindings, callback);
  }

  static updateEdge(__edgeId, edgeProperties, callback) {
    const properties = this._properties;
    const __graphName = `${properties.keyspace}_graph`;
    let script = `
    graph = ConfiguredGraphFactory.open(__graphName);
    g = graph.traversal();
    edge = g.E(__edgeId);
  `;
    Object.keys(edgeProperties).forEach((property) => {
      script += `edge.property('${property}', ${property});`;
    });
    script += 'edge';
    const bindings = _.defaults(edgeProperties, {
      __graphName,
      __edgeId,
    });
    this._execute_gremlin_script(script, bindings, callback);
  }

  static deleteEdge(__edgeId, callback) {
    const properties = this._properties;
    const __graphName = `${properties.keyspace}_graph`;
    const script = `
    graph = ConfiguredGraphFactory.open(__graphName);
    g = graph.traversal();
    edge = g.E(__edgeId);
    edge.drop();
  `;
    const bindings = {
      __graphName,
      __edgeId,
    };
    this._execute_gremlin_script(script, bindings, callback);
  }

  static graphQuery(query, params, callback) {
    const properties = this._properties;
    const __graphName = `${properties.keyspace}_graph`;
    const __vertexLabel = properties.table_name;
    let script = `
    graph = ConfiguredGraphFactory.open(__graphName);
    g = graph.traversal();
    vertices = g.V().hasLabel(__vertexLabel);
  `;
    script += query;
    const bindings = _.defaults(params, {
      __graphName,
      __vertexLabel,
    });
    this._execute_gremlin_query(script, bindings, callback);
  }

  static search(queryObject, callback) {
    const esClient = this.get_es_client();
    const indexName = `${this._properties.keyspace}_${this._properties.table_name}`;

    const query = _.defaults(queryObject, {
      index: indexName,
      type: this._properties.table_name,
    });
    esClient.search(query, (err, response) => {
      if (err) {
        callback(err);
        return;
      }
      callback(null, response);
    });
  }

  static find(queryObject, options, callback) {
    if (arguments.length === 2 && typeof options === 'function') {
      callback = options;
      options = {};
    }
    if (typeof callback !== 'function' && !options.return_query) {
      throw buildError('model.find.cberror');
    }

    const defaults = {
      raw: false,
      prepare: true,
    };

    options = _.defaultsDeep(options, defaults);

    // set raw true if select is used,
    // because casting to model instances may lead to problems
    if (options.select) options.raw = true;

    let queryParams = [];

    let query;
    try {
      const findQuery = this.get_find_query(queryObject, options);
      query = findQuery.query;
      queryParams = queryParams.concat(findQuery.params);
    } catch (e) {
      Parser.callbackOrThrow(e, callback);
      return {};
    }

    if (options.return_query) {
      return { query, params: queryParams };
    }

    const queryOptions = Normalizer.normalizeQueryOption(options);

    this._executeTableQuery(
      query,
      queryParams,
      queryOptions,
      (err, results) => {
        if (err) {
          callback(buildError('model.find.dberror', err));
          return;
        }
        if (!options.raw) {
          const ModelConstructor = this._properties.get_constructor();
          results = results.rows.map((res) => {
            delete res.columns;
            const o = new ModelConstructor(res);
            o._modified = {};
            return o;
          });
          callback(null, results);
        } else {
          results = results.rows.map((res) => {
            delete res.columns;
            return res;
          });
          callback(null, results);
        }
      },
    );

    return {};
  }

  static findOne(queryObject, options, callback) {
    if (arguments.length === 2 && typeof options === 'function') {
      callback = options;
      options = {};
    }
    if (typeof callback !== 'function' && !options.return_query) {
      throw buildError('model.find.cberror');
    }

    queryObject.$limit = 1;

    return this.find(queryObject, options, (err, results) => {
      if (err) {
        callback(err);
        return;
      }
      if (results.length > 0) {
        callback(null, results[0]);
        return;
      }
      callback();
    });
  }

  static update(queryObject, updateValues, options, callback) {
    if (arguments.length === 3 && typeof options === 'function') {
      callback = options;
      options = {};
    }

    const schema = this._properties.schema;

    const defaults = {
      prepare: true,
    };

    options = _.defaultsDeep(options, defaults);

    if (
      typeof schema.before_update === 'function' &&
      schema.before_update(queryObject, updateValues, options) === false
    ) {
      Parser.callbackOrThrow(buildError('model.update.before.error'), callback);
      return {};
    }

    const {
      updateClauses,
      queryParams,
      errorHappened,
    } = Parser.getUpdateValueExpression(this, schema, updateValues, callback);

    if (errorHappened) return {};

    let query = 'UPDATE "%s"';
    let finalParams = queryParams;
    if (_.isNumber(options.ttl)) {
      query += ' USING TTL ?';
      finalParams = [options.ttl].concat(finalParams);
    }
    query += ' SET %s %s';

    let where = '';
    try {
      const whereClause = Parser.getWhereClause(schema, queryObject);
      where = whereClause.query;
      finalParams = finalParams.concat(whereClause.params);
    } catch (e) {
      Parser.callbackOrThrow(e, callback);
      return {};
    }

    query = util.format(
      query,
      this._properties.table_name,
      updateClauses.join(', '),
      where,
    );

    if (options.conditions) {
      const ifClause = Parser.getIfClause(schema, options.conditions);
      if (ifClause.query) {
        query += util.format(' %s', ifClause.query);
        finalParams = finalParams.concat(ifClause.params);
      }
    } else if (options.if_exists) {
      query += ' IF EXISTS';
    }

    query += ';';

    if (options.return_query) {
      const returnObj = {
        query,
        params: finalParams,
        after_hook: () => {
          if (
            typeof schema.after_update === 'function' &&
            schema.after_update(queryObject, updateValues, options) === false
          ) {
            return buildError('model.update.after.error');
          }
          return true;
        },
      };
      return returnObj;
    }

    const queryOptions = Normalizer.normalizeQueryOption(options);

    this._execute_table_query(
      query,
      finalParams,
      queryOptions,
      (err, results) => {
        if (typeof callback === 'function') {
          if (err) {
            callback(buildError('model.update.dberror', err));
            return;
          }
          if (
            typeof schema.after_update === 'function' &&
            schema.after_update(queryObject, updateValues, options) === false
          ) {
            callback(buildError('model.update.after.error'));
            return;
          }
          callback(null, results);
        } else if (err) {
          throw buildError('model.update.dberror', err);
        } else if (
          typeof schema.after_update === 'function' &&
          schema.after_update(queryObject, updateValues, options) === false
        ) {
          throw buildError('model.update.after.error');
        }
      },
    );

    return {};
  }

  static delete(queryObject, options, callback) {
    if (arguments.length === 2 && typeof options === 'function') {
      callback = options;
      options = {};
    }

    const schema = this._properties.schema;

    const defaults = {
      prepare: true,
    };

    options = _.defaultsDeep(options, defaults);

    if (
      typeof schema.before_delete === 'function' &&
      schema.before_delete(queryObject, options) === false
    ) {
      Parser.callbackOrThrow(buildError('model.delete.before.error'), callback);
      return {};
    }

    let queryParams = [];

    let query = 'DELETE FROM "%s" %s;';
    let where = '';
    try {
      const whereClause = Parser.getWhereClause(schema, queryObject);
      where = whereClause.query;
      queryParams = queryParams.concat(whereClause.params);
    } catch (e) {
      Parser.callbackOrThrow(e, callback);
      return {};
    }

    query = util.format(query, this._properties.table_name, where);

    if (options.return_query) {
      const returnObj = {
        query,
        params: queryParams,
        after_hook: () => {
          if (
            typeof schema.after_delete === 'function' &&
            schema.after_delete(queryObject, options) === false
          ) {
            return buildError('model.delete.after.error');
          }
          return true;
        },
      };
      return returnObj;
    }

    const queryOptions = Normalizer.normalizeQueryOption(options);

    this._execute_table_query(
      query,
      queryParams,
      queryOptions,
      (err, results) => {
        if (typeof callback === 'function') {
          if (err) {
            callback(buildError('model.delete.dberror', err));
            return;
          }
          if (
            typeof schema.after_delete === 'function' &&
            schema.after_delete(queryObject, options) === false
          ) {
            callback(buildError('model.delete.after.error'));
            return;
          }
          callback(null, results);
        } else if (err) {
          throw buildError('model.delete.dberror', err);
        } else if (
          typeof schema.after_delete === 'function' &&
          schema.after_delete(queryObject, options) === false
        ) {
          throw buildError('model.delete.after.error');
        }
      },
    );

    return {};
  }

  static truncate(callback) {
    const properties = this._properties;
    const tableName = properties.table_name;

    const query = util.format('TRUNCATE TABLE "%s";', tableName);
    this._execute_table_query(query, [], callback);
  }

  // normal functions

  get_data_types() {
    return cql.types;
  }

  get_table_name() {
    return this.constructor.get_table_name();
  }

  get_keyspace_name() {
    return this.constructor.get_keyspace_name();
  }

  _get_default_value(fieldname) {
    const properties = this.constructor._properties;
    const schema = properties.schema;

    if (
      _.isPlainObject(schema.fields[fieldname]) &&
      schema.fields[fieldname].default !== undefined
    ) {
      if (typeof schema.fields[fieldname].default === 'function') {
        return schema.fields[fieldname].default.call(this);
      }
      return schema.fields[fieldname].default;
    }
    return undefined;
  }

  validate(propertyName, value) {
    value = value || this[propertyName];
    this._validators = this._validators || {};
    return Schemer.getValidationMessage(
      this._validators[propertyName] || [],
      value,
    );
  }

  save(options, callback) {
    if (arguments.length === 1 && typeof options === 'function') {
      callback = options;
      options = {};
    }

    const properties = this.constructor._properties;
    const schema = properties.schema;

    const defaults = {
      prepare: true,
    };

    options = _.defaultsDeep(options, defaults);

    if (
      typeof schema.before_save === 'function' &&
      schema.before_save(this, options) === false
    ) {
      Parser.callbackOrThrow(buildError('model.save.before.error'), callback);
      return {};
    }

    const {
      identifiers,
      values,
      queryParams,
      errorHappened,
    } = Parser.getSaveValueExpression(this, schema, callback);

    if (errorHappened) return {};

    let query = util.format(
      'INSERT INTO "%s" ( %s ) VALUES ( %s )',
      properties.table_name,
      identifiers.join(' , '),
      values.join(' , '),
    );

    if (options.if_not_exist) query += ' IF NOT EXISTS';

    let finalParams = queryParams;
    if (_.isNumber(options.ttl)) {
      query += ' USING TTL ?';
      finalParams = finalParams.concat([options.ttl]);
    }

    query += ';';

    if (options.return_query) {
      const returnObj = {
        query,
        params: finalParams,
        after_hook: () => {
          if (
            typeof schema.after_save === 'function' &&
            schema.after_save(this, options) === false
          ) {
            return buildError('model.save.after.error');
          }
          return true;
        },
      };
      return returnObj;
    }

    const queryOptions = Normalizer.normalizeQueryOption(options);

    this.constructor._execute_table_query(
      query,
      finalParams,
      queryOptions,
      (err, result) => {
        if (typeof callback === 'function') {
          if (err) {
            callback(buildError('model.save.dberror', err));
            return;
          }
          if (
            !options.if_not_exist ||
            (result.rows && result.rows[0] && result.rows[0]['[applied]'])
          ) {
            this._modified = {};
          }
          if (
            typeof schema.after_save === 'function' &&
            schema.after_save(this, options) === false
          ) {
            callback(buildError('model.save.after.error'));
            return;
          }
          callback(null, result);
        } else if (err) {
          throw buildError('model.save.dberror', err);
        } else if (
          typeof schema.after_save === 'function' &&
          schema.after_save(this, options) === false
        ) {
          throw buildError('model.save.after.error');
        }
      },
    );

    return {};
  }

  delete(options, callback) {
    if (arguments.length === 1 && typeof options === 'function') {
      callback = options;
      options = {};
    }

    const schema = this.constructor._properties.schema;
    const deleteQuery = {};

    for (let i = 0; i < schema.key.length; i++) {
      const fieldKey = schema.key[i];
      if (_.isArray(fieldKey)) {
        for (let j = 0; j < fieldKey.length; j++) {
          deleteQuery[fieldKey[j]] = this[fieldKey[j]];
        }
      } else {
        deleteQuery[fieldKey] = this[fieldKey];
      }
    }

    return this.delete(deleteQuery, options, callback);
  }

  toJSON() {
    const object = {};
    const schema = this.constructor._properties.schema;

    Object.keys(schema.fields).forEach((field) => {
      object[field] = this[field];
    });

    return object;
  }

  isModified(propName) {
    if (propName) {
      return Object.prototype.hasOwnProperty.call(this._modified, propName);
    }
    return Object.keys(this._modified).length !== 0;
  }
}
