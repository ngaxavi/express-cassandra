const util = require('util');
const debug = require('debug')('express-cassandra');

export default class KeyspaceBuilder {
  constructor(private readonly _client) {}

  generateReplicationText(replicationOptions) {
    if (typeof replicationOptions === 'string') {
      return replicationOptions;
    }

    const properties = [];
    Object.keys(replicationOptions).forEach((k: string) => {
      properties.push(util.format("'%s': '%s'", k, replicationOptions[k]));
    });

    return util.format('{%s}', properties.join(','));
  }

  createKeyspace(keyspaceName, defaultReplicationStrategy, callback) {
    const replicationText = this.generateReplicationText(
      defaultReplicationStrategy,
    );

    const query = util.format(
      'CREATE KEYSPACE IF NOT EXISTS "%s" WITH REPLICATION = %s;',
      keyspaceName,
      replicationText,
    );
    debug('executing query: %s', query);
    this._client.execute(query, (err) => {
      this._client.shutdown(() => {
        callback(err);
      });
    });
  }

  alterKeyspace(keyspaceName, defaultReplicationStrategy, callback) {
    const replicationText = this.generateReplicationText(
      defaultReplicationStrategy,
    );

    const query = util.format(
      'ALTER KEYSPACE "%s" WITH REPLICATION = %s;',
      keyspaceName,
      replicationText,
    );
    debug('executing query: %s', query);
    this._client.execute(query, (err) => {
      this._client.shutdown(() => {
        // eslint-disable-next-line no-console
        console.warn(
          'WARN: KEYSPACE ALTERED! Run the `nodetool repair` command on each affected node.',
        );
        callback(err);
      });
    });
  }

  getKeyspace(keyspaceName, callback) {
    const query = util.format(
      "SELECT * FROM system_schema.keyspaces WHERE keyspace_name = '%s';",
      keyspaceName,
    );
    debug('executing query: %s', query);
    this._client.execute(query, (err, result) => {
      if (err) {
        this._client.shutdown(() => {
          callback(err);
        });
        return;
      }

      if (result.rows && result.rows.length > 0) {
        callback(null, result.rows[0]);
        return;
      }

      callback();
    });
  }
}
