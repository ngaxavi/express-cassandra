import { buildError } from '../orm/apollo_error';
import { ElassandraBuilder } from './elassandra';
import { Schemer } from '../validators/schema';
import { Parser } from '../utils/parser';
import { Normalizer } from '../utils/normalizer';

const _ = require('lodash');
const async = require('async');
const util = require('util');
const objectHash = require('object-hash');
const readlineSync = require('readline-sync');
const deepDiff = require('deep-diff').diff;

export class TableBuilder {
  private readonly _esBuilder;

  constructor(private readonly _driver, private readonly _properties) {
    if (this._properties.esclient) {
      this._esBuilder = new ElassandraBuilder(this._properties.esclient);
    }
  }

  _confirmMigration(message) {
    let permission = 'y';
    if (message && !this._properties.disableTTYConfirmation) {
      permission = readlineSync.question(
        util.format('Migration: %s (y/n): ', message),
      );
    }
    return permission.toLowerCase();
  }

  getTable(callback) {
    const properties = this._properties;
    const keyspaceName = properties.keyspace;
    const tableName = properties.table_name;
    const dbSchema: any = { fields: {}, typeMaps: {}, staticMaps: {} };
    const query =
      'SELECT * FROM system_schema.columns WHERE table_name = ? AND keyspace_name = ?;';

    this._driver.execute_query(
      query,
      [tableName, keyspaceName],
      (err, resultColumns) => {
        if (err) {
          callback(buildError('model.tablecreation.dbschemaquery', err));
          return;
        }

        if (!resultColumns.rows || resultColumns.rows.length === 0) {
          callback();
          return;
        }

        for (let r = 0; r < resultColumns.rows.length; r++) {
          const row = resultColumns.rows[r];

          dbSchema.fields[row.column_name] = Parser.extractType(row.type);

          const typeMapDef = Parser.extractTypeDef(row.type);
          if (typeMapDef.length > 0) {
            dbSchema.typeMaps[row.column_name] = typeMapDef;
          }

          if (row.kind === 'partition_key') {
            if (!dbSchema.key) dbSchema.key = [[]];
            dbSchema.key[0][row.position] = row.column_name;
          } else if (row.kind === 'clustering') {
            if (!dbSchema.key) dbSchema.key = [[]];
            if (!dbSchema.clustering_order) dbSchema.clustering_order = {};

            dbSchema.key[row.position + 1] = row.column_name;
            if (
              row.clustering_order &&
              row.clustering_order.toLowerCase() === 'desc'
            ) {
              dbSchema.clustering_order[row.column_name] = 'DESC';
            } else {
              dbSchema.clustering_order[row.column_name] = 'ASC';
            }
          } else if (row.kind === 'static') {
            dbSchema.staticMaps[row.column_name] = true;
          }
        }

        callback(null, dbSchema);
      },
    );
  }

  getTableSchema(callback) {
    this.getTable((err, dbSchema) => {
      if (err) {
        callback(err);
        return;
      }
      if (!dbSchema) {
        callback();
        return;
      }
      this.getIndexes((err1, indexSchema) => {
        if (err1) {
          callback(err1);
          return;
        }
        this.getMviews((err2, viewSchema) => {
          if (err2) {
            callback(err2);
            return;
          }
          Object.assign(dbSchema, indexSchema, viewSchema);
          callback(null, dbSchema);
        });
      });
    });
  }

  createTable(schema, callback) {
    const tableName = this._properties.table_name;
    const rows = [];
    let fieldType;
    Object.keys(schema.fields).forEach((k) => {
      if (schema.fields[k].virtual) {
        return;
      }
      let segment = '';
      fieldType = Schemer.getFieldType(schema, k);
      if (schema.fields[k].typeDef) {
        segment = util.format(
          '"%s" %s%s',
          k,
          fieldType,
          schema.fields[k].typeDef,
        );
      } else {
        segment = util.format('"%s" %s', k, fieldType);
      }

      if (schema.fields[k].static) {
        segment += ' STATIC';
      }

      rows.push(segment);
    });

    const clauses = Parser.getPrimaryKeyClauses(schema);

    const query = util.format(
      'CREATE TABLE IF NOT EXISTS "%s" (%s , PRIMARY KEY((%s)%s))%s;',
      tableName,
      rows.join(' , '),
      clauses.partitionKeyClause,
      clauses.clusteringKeyClause,
      clauses.clusteringOrderClause,
    );

    this._driver.execute_definition_query(query, (err, result) => {
      if (err) {
        callback(buildError('model.tablecreation.dbcreate', err));
        return;
      }
      callback(null, result);
    });
  }

  alterTable(operation, fieldName, type, callback) {
    const tableName = this._properties.table_name;
    if (operation === 'ALTER') type = util.format('TYPE %s', type);
    else if (operation === 'DROP') type = '';

    const query = util.format(
      'ALTER TABLE "%s" %s "%s" %s;',
      tableName,
      operation,
      fieldName,
      type,
    );
    this._driver.execute_definition_query(query, callback);
  }

  _dropTable(tableName, callback) {
    const query = util.format('DROP TABLE IF EXISTS "%s";', tableName);
    this._driver.execute_definition_query(query, (err) => {
      if (err) {
        callback(buildError('model.tablecreation.dbdrop', err));
        return;
      }
      callback();
    });
  }

  dropTable(materializedViews, callback) {
    const tableName = this._properties.table_name;
    const message = util.format(
      'Schema for table "%s" has changed in a way where alter migration is not possible, all data in the table will be lost, are you sure you want to drop the table?',
      tableName,
    );
    const permission = this._confirmMigration(message);
    if (permission !== 'y') {
      callback(
        buildError(
          'model.tablecreation.schemamismatch',
          tableName,
          'migration suspended, please apply the change manually',
        ),
      );
      return;
    }
    if (!materializedViews) {
      this._dropTable(tableName, callback);
      return;
    }

    const mviews = Object.keys(materializedViews);
    this.dropMviews(mviews, (err) => {
      if (err) {
        callback(err);
        return;
      }

      this._dropTable(tableName, callback);
    });
  }

  dropRecreateTable(modelSchema, materializedViews, callback) {
    if (this._esBuilder) {
      const indexName = `${this._properties.keyspace}_${this._properties.table_name}`;
      this._esBuilder.delete_index(indexName, () => {
        this.dropTable(materializedViews, (err1) => {
          if (err1) {
            callback(err1);
            return;
          }
          this.createTable(modelSchema, callback);
        });
      });
      return;
    }
    this.dropTable(materializedViews, (err1) => {
      if (err1) {
        callback(err1);
        return;
      }
      this.createTable(modelSchema, callback);
    });
  }

  getIndexes(callback) {
    const properties = this._properties;
    const keyspaceName = properties.keyspace;
    const tableName = properties.table_name;
    const dbSchema: any = {};
    const query =
      'SELECT * FROM system_schema.indexes WHERE table_name = ? AND keyspace_name = ?;';

    this._driver.execute_query(
      query,
      [tableName, keyspaceName],
      (err, resultIndexes) => {
        if (err) {
          callback(buildError('model.tablecreation.dbschemaquery', err));
          return;
        }

        for (let r = 0; r < resultIndexes.rows.length; r++) {
          const row = resultIndexes.rows[r];

          if (row.index_name && row.options.target) {
            const indexOptions = row.options;
            let target = indexOptions.target;
            target = target.replace(/["\s]/g, '');
            delete indexOptions.target;

            // keeping track of index names to drop index when needed
            if (!dbSchema.index_names) dbSchema.index_names = {};

            if (row.kind === 'CUSTOM') {
              const using = indexOptions.class_name;
              delete indexOptions.class_name;

              if (!dbSchema.custom_indexes) dbSchema.custom_indexes = [];
              const customIndexObject = {
                on: target,
                using,
                options: indexOptions,
              };
              dbSchema.custom_indexes.push(customIndexObject);
              dbSchema.index_names[objectHash(customIndexObject)] =
                row.index_name;
            } else {
              if (!dbSchema.indexes) dbSchema.indexes = [];
              dbSchema.indexes.push(target);
              dbSchema.index_names[target] = row.index_name;
            }
          }
        }

        callback(null, dbSchema);
      },
    );
  }

  _createIndexQuery(tableName, indexName) {
    let query;
    const indexExpression = indexName.replace(/["\s]/g, '').split(/[()]/g);
    if (indexExpression.length > 1) {
      indexExpression[0] = indexExpression[0].toLowerCase();
      query = util.format(
        'CREATE INDEX IF NOT EXISTS ON "%s" (%s("%s"));',
        tableName,
        indexExpression[0],
        indexExpression[1],
      );
    } else {
      query = util.format(
        'CREATE INDEX IF NOT EXISTS ON "%s" ("%s");',
        tableName,
        indexExpression[0],
      );
    }
    return query;
  }

  createIndexes(indexes, callback) {
    const tableName = this._properties.table_name;
    async.eachSeries(
      indexes,
      (idx, next) => {
        const query = this._createIndexQuery(tableName, idx);
        this._driver.execute_definition_query(query, (err, result) => {
          if (err) next(buildError('model.tablecreation.dbindexcreate', err));
          else next(null, result);
        });
      },
      callback,
    );
  }

  _createCustomIndexQuery(tableName, customIndex) {
    let query = util.format(
      'CREATE CUSTOM INDEX IF NOT EXISTS ON "%s" ("%s") USING \'%s\'',
      tableName,
      customIndex.on,
      customIndex.using,
    );

    if (Object.keys(customIndex.options).length > 0) {
      query += ' WITH OPTIONS = {';
      Object.keys(customIndex.options).forEach((key) => {
        query += util.format("'%s': '%s', ", key, customIndex.options[key]);
      });
      query = query.slice(0, -2);
      query += '}';
    }

    query += ';';

    return query;
  }

  createCustomIndexes(customIndexes, callback) {
    const tableName = this._properties.table_name;
    async.eachSeries(
      customIndexes,
      (idx, next) => {
        const query = this._createCustomIndexQuery(tableName, idx);
        this._driver.execute_definition_query(query, (err, result) => {
          if (err) next(buildError('model.tablecreation.dbindexcreate', err));
          else next(null, result);
        });
      },
      callback,
    );
  }

  dropIndexes(indexes, callback) {
    async.each(
      indexes,
      (idx, next) => {
        const query = util.format('DROP INDEX IF EXISTS "%s";', idx);
        this._driver.execute_definition_query(query, next);
      },
      (err) => {
        if (err) callback(buildError('model.tablecreation.dbindexdrop', err));
        else callback();
      },
    );
  }

  getMviews(callback) {
    const properties = this._properties;
    const keyspaceName = properties.keyspace;
    const tableName = properties.table_name;
    const dbSchema: any = {};
    let query =
      'SELECT view_name,base_table_name,where_clause FROM system_schema.views WHERE keyspace_name=? AND base_table_name=? ALLOW FILTERING;';

    this._driver.execute_query(
      query,
      [keyspaceName, tableName],
      (err, resultViews) => {
        if (err) {
          callback(buildError('model.tablecreation.dbschemaquery', err));
          return;
        }

        for (let r = 0; r < resultViews.rows.length; r++) {
          const row = resultViews.rows[r];

          if (row.view_name) {
            if (!dbSchema.materializedViews) dbSchema.materializedViews = {};
            dbSchema.materializedViews[row.view_name] = {
              where_clause: row.where_clause,
            };
          }
        }

        if (!dbSchema.materializedViews) {
          callback(null, dbSchema);
          return;
        }

        query =
          'SELECT * FROM system_schema.columns WHERE keyspace_name=? and table_name IN ?;';

        const viewNames = Object.keys(dbSchema.materializedViews);
        this._driver.execute_query(
          query,
          [keyspaceName, viewNames],
          (err1, resultMatViews) => {
            if (err1) {
              callback(buildError('model.tablecreation.dbschemaquery', err1));
              return;
            }

            for (let r = 0; r < resultMatViews.rows.length; r++) {
              const row = resultMatViews.rows[r];

              if (!dbSchema.materializedViews[row.table_name].select) {
                dbSchema.materializedViews[row.table_name].select = [];
              }

              dbSchema.materializedViews[row.table_name].select.push(
                row.column_name,
              );

              if (row.kind === 'partition_key') {
                if (!dbSchema.materializedViews[row.table_name].key) {
                  dbSchema.materializedViews[row.table_name].key = [[]];
                }

                dbSchema.materializedViews[row.table_name].key[0][
                  row.position
                ] = row.column_name;
              } else if (row.kind === 'clustering') {
                if (!dbSchema.materializedViews[row.table_name].key) {
                  dbSchema.materializedViews[row.table_name].key = [[]];
                }
                if (
                  !dbSchema.materializedViews[row.table_name].clustering_order
                ) {
                  dbSchema.materializedViews[
                    row.table_name
                  ].clustering_order = {};
                }

                dbSchema.materializedViews[row.table_name].key[
                  row.position + 1
                ] = row.column_name;
                if (
                  row.clustering_order &&
                  row.clustering_order.toLowerCase() === 'desc'
                ) {
                  dbSchema.materializedViews[row.table_name].clustering_order[
                    row.column_name
                  ] = 'DESC';
                } else {
                  dbSchema.materializedViews[row.table_name].clustering_order[
                    row.column_name
                  ] = 'ASC';
                }
              }
            }

            callback(null, dbSchema);
          },
        );
      },
    );
  }

  _createMaterializedViewQuery(tableName, viewName, viewSchema) {
    const rows = [];

    for (let k = 0; k < viewSchema.select.length; k++) {
      if (viewSchema.select[k] === '*')
        rows.push(util.format('%s', viewSchema.select[k]));
      else rows.push(util.format('"%s"', viewSchema.select[k]));
    }

    const whereClause =
      viewSchema.where_clause ||
      Parser.getMviewWhereClause(this._properties.schema, viewSchema);
    const clauses = Parser.getPrimaryKeyClauses(viewSchema);

    return util.format(
      'CREATE MATERIALIZED VIEW IF NOT EXISTS "%s" AS SELECT %s FROM "%s" WHERE %s PRIMARY KEY((%s)%s)%s;',
      viewName,
      rows.join(' , '),
      tableName,
      whereClause,
      clauses.partitionKeyClause,
      clauses.clusteringKeyClause,
      clauses.clusteringOrderClause,
    );
  }

  createMviews(materializedViews, callback) {
    const properties = this._properties;
    const tableName = properties.table_name;
    async.eachSeries(
      Object.keys(materializedViews),
      (viewName, next) => {
        const query = this._createMaterializedViewQuery(
          tableName,
          viewName,
          materializedViews[viewName],
        );
        this._driver.execute_definition_query(query, (err, result) => {
          if (err) next(buildError('model.tablecreation.matviewcreate', err));
          else next(null, result);
        });
      },
      callback,
    );
  }

  dropMviews(mviews, callback) {
    async.each(
      mviews,
      (view, next) => {
        const query = util.format(
          'DROP MATERIALIZED VIEW IF EXISTS "%s";',
          view,
        );
        this._driver.execute_definition_query(query, next);
      },
      (err) => {
        if (err) callback(buildError('model.tablecreation.matviewdrop', err));
        else callback();
      },
    );
  }

  _applyAlterOperations(
    alterOperations,
    dbSchema,
    normalizedModelSchema,
    normalizedDBSchema,
    callback,
  ) {
    // it should create/drop indexes/custom_indexes/materializedViews that are added/removed in model schema
    // remove common indexes/custom_indexes/materializedViews from normalizedModelSchema and normalizedDBSchema
    // then drop all remaining indexes/custom_indexes/materializedViews from normalizedDBSchema
    // and add all remaining indexes/custom_indexes/materializedViews from normalizedModelSchema
    const properties = this._properties;
    const tableName = properties.table_name;
    const addedIndexes = _.difference(
      normalizedModelSchema.indexes,
      normalizedDBSchema.indexes,
    );
    const removedIndexes = _.difference(
      normalizedDBSchema.indexes,
      normalizedModelSchema.indexes,
    );
    const removedIndexNames = [];
    removedIndexes.forEach((removedIndex) => {
      removedIndexNames.push(dbSchema.index_names[removedIndex]);
    });

    const addedCustomIndexes = _.filter(
      normalizedModelSchema.custom_indexes,
      (obj) => !_.find(normalizedDBSchema.custom_indexes, obj),
    );
    const removedCustomIndexes = _.filter(
      normalizedDBSchema.custom_indexes,
      (obj) => !_.find(normalizedModelSchema.custom_indexes, obj),
    );
    removedCustomIndexes.forEach((removedIndex) => {
      removedIndexNames.push(dbSchema.index_names[objectHash(removedIndex)]);
    });

    const addedMaterializedViewsNames = _.filter(
      Object.keys(normalizedModelSchema.materializedViews),
      (viewName) =>
        !_.isEqual(
          normalizedDBSchema.materializedViews[viewName],
          normalizedModelSchema.materializedViews[viewName],
        ),
    );

    const removedMaterializedViewNames = _.filter(
      Object.keys(normalizedDBSchema.materializedViews),
      (viewName) =>
        !_.isEqual(
          normalizedDBSchema.materializedViews[viewName],
          normalizedModelSchema.materializedViews[viewName],
        ),
    );

    const addedMaterializedViews = {};
    addedMaterializedViewsNames.forEach((viewName) => {
      addedMaterializedViews[viewName] =
        normalizedModelSchema.materializedViews[viewName];
    });

    // remove altered materialized views
    if (removedMaterializedViewNames.length > 0) {
      const message = util.format(
        'Schema for table "%s" has removed materializedViews: %j, are you sure you want to drop them?',
        tableName,
        removedMaterializedViewNames,
      );
      const permission = this._confirmMigration(message);
      if (permission !== 'y') {
        callback(
          buildError(
            'model.tablecreation.schemamismatch',
            tableName,
            'migration suspended, please apply the change manually',
          ),
        );
        return;
      }
    }

    this.dropMviews(removedMaterializedViewNames, (err2) => {
      if (err2) {
        callback(err2);
        return;
      }

      if (removedIndexNames.length > 0) {
        const message = util.format(
          'Schema for table "%s" has removed indexes: %j, are you sure you want to drop them?',
          tableName,
          removedIndexNames,
        );
        const permission = this._confirmMigration(message);
        if (permission !== 'y') {
          callback(
            buildError(
              'model.tablecreation.schemamismatch',
              tableName,
              'migration suspended, please apply the change manually',
            ),
          );
          return;
        }
      }

      // remove altered indexes by index name
      this.dropIndexes(removedIndexNames, (err3) => {
        if (err3) {
          callback(err3);
          return;
        }

        // now apply alterOperations here
        async.eachSeries(
          alterOperations,
          (alterOperation, next) => {
            const permission = this._confirmMigration(alterOperation.message);
            if (permission !== 'y') {
              callback(
                buildError(
                  'model.tablecreation.schemamismatch',
                  tableName,
                  'migration suspended, please apply the change manually',
                ),
              );
              return;
            }
            this.alterTable(
              alterOperation.operation,
              alterOperation.fieldName,
              alterOperation.type,
              next,
            );
          },
          (err4) => {
            if (err4) {
              callback(err4);
              return;
            }

            // add altered indexes
            // eslint-disable-next-line max-nested-callbacks
            this.createIndexes(addedIndexes, (err5) => {
              if (err5) {
                callback(err5);
                return;
              }

              // add altered custom indexes
              // eslint-disable-next-line max-nested-callbacks
              this.createCustomIndexes(addedCustomIndexes, (err6) => {
                if (err6) {
                  callback(err6);
                  return;
                }

                // add altered materializedViews
                this.createMviews(addedMaterializedViews, callback);
              });
            });
          },
        );
      });
    });
  }

  initAlterOperations(
    modelSchema,
    dbSchema,
    normalizedModelSchema,
    normalizedDBSchema,
    callback,
  ) {
    const properties = this._properties;
    const tableName = properties.table_name;
    const alterOperations = [];
    const differences = deepDiff(
      normalizedDBSchema.fields,
      normalizedModelSchema.fields,
    );
    let droppedFields = false;
    async.eachSeries(
      differences,
      (diff, next) => {
        const fieldName = diff.path[0];
        if (diff.kind === 'N') {
          const message = util.format(
            'Schema for table "%s" has added field "%s", are you sure you want to alter to add the field?',
            tableName,
            fieldName,
          );
          alterOperations.push({
            fieldName,
            message,
            operation: 'ADD',
            type: Parser.extractAlteredType(normalizedModelSchema, diff),
          });
          next();
          return;
        }
        if (diff.kind === 'D') {
          const message = util.format(
            'Schema for table "%s" has removed field "%s", all data in the field will lost, are you sure you want to alter to drop the field?',
            tableName,
            fieldName,
          );
          alterOperations.push({
            fieldName,
            message,
            operation: 'DROP',
          });
          droppedFields = true;
          Normalizer.removeDependentViewsFromNormalizedSchema(
            normalizedDBSchema,
            dbSchema,
            fieldName,
          );
          next();
          return;
        }
        if (diff.kind === 'E') {
          // check if the alter field type is possible, otherwise try D and then N
          if (diff.path[1] === 'type') {
            // check if field part of primary key
            if (
              normalizedDBSchema.key[0].includes(fieldName) ||
              normalizedDBSchema.key.indexOf(fieldName) > 0
            ) {
              // alter field type impossible
              next(new Error('alter_impossible'));
            } else if (
              [
                'text',
                'ascii',
                'bigint',
                'boolean',
                'decimal',
                'double',
                'float',
                'inet',
                'int',
                'timestamp',
                'timeuuid',
                'uuid',
                'varchar',
                'varint',
              ].includes(diff.lhs) &&
              diff.rhs === 'blob'
            ) {
              // alter field type possible
              const message = util.format(
                'Schema for table "%s" has new type for field "%s", are you sure you want to alter to update the field type?',
                tableName,
                fieldName,
              );
              alterOperations.push({
                fieldName,
                message,
                operation: 'ALTER',
                type: diff.rhs,
              });
              next();
            } else if (diff.lhs === 'int' && diff.rhs === 'varint') {
              // alter field type possible
              const message = util.format(
                'Schema for table "%s" has new type for field "%s", are you sure you want to alter to update the field type?',
                tableName,
                fieldName,
              );
              alterOperations.push({
                fieldName,
                message,
                operation: 'ALTER',
                type: diff.rhs,
              });
              next();
            } else if (diff.lhs === 'timeuuid' && diff.rhs === 'uuid') {
              // alter field type possible
              const message = util.format(
                'Schema for table "%s" has new type for field "%s", are you sure you want to alter to update the field type?',
                tableName,
                fieldName,
              );
              alterOperations.push({
                fieldName,
                message,
                operation: 'ALTER',
                type: diff.rhs,
              });
              next();
            } else {
              // alter type impossible
              const message = util.format(
                'Schema for table "%s" has new type for field "%s", all data in the field will be lost, are you sure you want to drop the field & recreate it?',
                tableName,
                fieldName,
              );
              alterOperations.push({
                fieldName,
                message,
                operation: 'DROP',
              });
              alterOperations.push({
                fieldName,
                operation: 'ADD',
                type: Parser.extractAlteredType(normalizedModelSchema, diff),
              });
              droppedFields = true;
              Normalizer.removeDependentViewsFromNormalizedSchema(
                normalizedDBSchema,
                dbSchema,
                fieldName,
              );
              next();
            }
          } else {
            // alter type impossible
            const message = util.format(
              'Schema for table "%s" has new type for field "%s", all data in the field will be lost, are you sure you want to drop the field & recreate it?',
              tableName,
              fieldName,
            );
            alterOperations.push({
              fieldName,
              message,
              operation: 'DROP',
            });
            alterOperations.push({
              fieldName,
              operation: 'ADD',
              type: Parser.extractAlteredType(normalizedModelSchema, diff),
            });
            droppedFields = true;
            Normalizer.removeDependentViewsFromNormalizedSchema(
              normalizedDBSchema,
              dbSchema,
              fieldName,
            );
            next();
          }
          return;
        }

        next();
      },
      (err) => {
        if (err) {
          callback(err);
          return;
        }
        if (droppedFields && this._esBuilder) {
          const indexName = `${properties.keyspace}_${properties.table_name}`;
          this._esBuilder.delete_index(indexName, () => {
            this._applyAlterOperations(
              alterOperations,
              dbSchema,
              normalizedModelSchema,
              normalizedDBSchema,
              callback,
            );
          });
          return;
        }
        this._applyAlterOperations(
          alterOperations,
          dbSchema,
          normalizedModelSchema,
          normalizedDBSchema,
          callback,
        );
      },
    );
  }
}
