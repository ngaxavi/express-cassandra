const _ = require('lodash');

const debug = require('debug')('express-cassandra');

export class Driver {

  constructor(private readonly properties) {}

  ensureInit(callback) {
    if (!this.properties.cql) {
      this.properties.init(callback);
    } else {
      callback();
    }
  }

  executeDefinitionQuery(query, callback) {
    this.ensureInit((err) => {
      if (err) {
        callback(err);
        return;
      }
      debug('executing definition query: %s', query);
      const conn = this.properties.define_connection;
      conn.execute(query, [], { prepare: false, fetchSize: 0 }, callback);
    });
  }

  executeQuery(query, params, options, callback) {
    if (arguments.length === 3) {
      callback = options;
      options = {};
    }

    const defaults = {
      prepare: true,
    };

    options = _.defaultsDeep(options, defaults);

    this.ensureInit((err) => {
      if (err) {
        callback(err);
        return;
      }
      debug('executing query: %s with params: %j', query, params);
      this.properties.cql.execute(query, params, options, (err1, result) => {
        if (err1 && err1.code === 8704) {
          this.executeDefinitionQuery(query, callback);
        } else {
          callback(err1, result);
        }
      });
    });
  }

  executeBatch(queries, options, callback) {
    if (arguments.length === 2) {
      callback = options;
      options = {};
    }

    const defaults = {
      prepare: true,
    };

    options = _.defaultsDeep(options, defaults);

    this.ensureInit((err) => {
      if (err) {
        callback(err);
        return;
      }
      debug('executing batch queries: %j', queries);
      this.properties.cql.batch(queries, options, callback);
    });
  }

  execute_eachRow(query, params, options, onReadable, callback) {
    this.ensureInit((err) => {
      if (err) {
        callback(err);
        return;
      }
      debug('executing eachRow query: %s with params: %j', query, params);
      this.properties.cql.eachRow(query, params, options, onReadable, callback);
    });
  }

  execute_stream(query, params, options, onReadable, callback) {
    this.ensureInit((err) => {
      if (err) {
        callback(err);
        return;
      }
      debug('executing stream query: %s with params: %j', query, params);
      this.properties.cql.stream(query, params, options).on('readable', onReadable).on('end', callback);
    });
  }
}

