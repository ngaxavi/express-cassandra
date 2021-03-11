const debug = require('debug')('express-cassandra');

export class ElassandraBuilder {
  constructor(private readonly _client) {}

  createIndex(keyspaceName, indexName, callback) {
    debug('creating elassandra index: %s', indexName);
    this._client.indices.create(
      {
        index: indexName,
        body: {
          settings: {
            keyspace: keyspaceName,
          },
        },
      },
      (err) => {
        if (err) {
          callback(err);
          return;
        }

        callback();
      },
    );
  }

  checkIndexExist(indexName, callback) {
    debug('check for elassandra index: %s', indexName);
    this._client.indices.exists({ index: indexName }, (err, res) => {
      if (err) {
        callback(err);
        return;
      }

      callback(null, res);
    });
  }

  assertIndex(keyspaceName, indexName, callback) {
    this.checkIndexExist(indexName, (err, exist) => {
      if (err) {
        callback(err);
        return;
      }

      if (!exist) {
        this.createIndex(keyspaceName, indexName, callback);
        return;
      }

      callback();
    });
  }

  deleteIndex(indexName, callback) {
    debug('removing elassandra index: %s', indexName);
    this._client.indices.delete(
      {
        index: indexName,
      },
      (err) => {
        if (err) {
          callback(err);
          return;
        }

        callback();
      },
    );
  }

  putMapping(indexName, mappingName, mappingBody, callback) {
    debug('syncing elassandra mapping: %s', mappingName);
    this._client.indices.putMapping(
      {
        index: indexName,
        type: mappingName,
        body: mappingBody,
      },
      (err) => {
        if (err) {
          callback(err);
          return;
        }

        callback();
      },
    );
  }
}
