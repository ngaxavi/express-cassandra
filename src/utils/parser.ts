import { TYPE_MAP } from '../validators/datatypes';

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

import { buildError } from '../orm/apollo_error';
import { Schemer } from '../validators/schema';

export class Parser {
  static callbackOrThrow(err, callback) {
    if (typeof callback === 'function') {
      callback(err);
      return;
    }
    throw err;
  }

  static extractType(val) {
    // decompose composite types
    const decomposed = val ? val.replace(/[\s]/g, '').split(/[<,>]/g) : [''];

    for (let d = 0; d < decomposed.length; d++) {
      if (_.has(TYPE_MAP, decomposed[d])) {
        return decomposed[d];
      }
    }

    return val;
  }

  static extractTypeDef(val) {
    // decompose composite types
    let decomposed = val ? val.replace(/[\s]/g, '') : '';
    decomposed = decomposed.substr(
      decomposed.indexOf('<'),
      decomposed.length - decomposed.indexOf('<'),
    );

    return decomposed;
  }

  static extractAlteredType(normalizedModelSchema, diff) {
    const fieldName = diff.path[0];
    let type = '';
    if (diff.path.length > 1) {
      if (diff.path[1] === 'type') {
        type = diff.rhs;
        if (normalizedModelSchema.fields[fieldName].typeDef) {
          type += normalizedModelSchema.fields[fieldName].typeDef;
        }
      } else {
        type = normalizedModelSchema.fields[fieldName].type;
        type += diff.rhs;
      }
    } else {
      type = diff.rhs.type;
      if (diff.rhs.typeDef) type += diff.rhs.typeDef;
    }
    return type;
  }

  static getDbValueExpression(schema, fieldName, fieldValue) {
    if (fieldValue == null || fieldValue === cql.types.unset) {
      return { query_segment: '?', parameter: fieldValue };
    }

    if (_.isPlainObject(fieldValue) && fieldValue.$db_function) {
      return fieldValue.$db_function;
    }

    const fieldType = Schemer.getFieldType(schema, fieldName);
    const validators = Schemer.getValidators(schema, fieldName);

    if (
      _.isArray(fieldValue) &&
      fieldType !== 'list' &&
      fieldType !== 'set' &&
      fieldType !== 'frozen'
    ) {
      const val = fieldValue.map((v) => {
        const dbVal = Parser.getDbValueExpression(schema, fieldName, v);

        if (_.isPlainObject(dbVal) && dbVal.query_segment)
          return dbVal.parameter;
        return dbVal;
      });

      return { query_segment: '?', parameter: val };
    }

    const validationMessage = Schemer.getValidationMessage(
      validators,
      fieldValue,
    );
    if (typeof validationMessage === 'function') {
      throw buildError(
        'model.validator.invalidvalue',
        validationMessage(fieldValue, fieldName, fieldType),
      );
    }

    if (fieldType === 'counter') {
      let counterQuerySegment = util.format('"%s"', fieldName);
      if (fieldValue >= 0) counterQuerySegment += ' + ?';
      else counterQuerySegment += ' - ?';
      fieldValue = Math.abs(fieldValue);
      return { query_segment: counterQuerySegment, parameter: fieldValue };
    }

    return { query_segment: '?', parameter: fieldValue };
  }

  static unsetNotAllowed(operation, schema, fieldName, callback) {
    if (Schemer.isPrimaryKeyField(schema, fieldName)) {
      this.callbackOrThrow(
        buildError(`model.${operation}.unsetkey`, fieldName),
        callback,
      );
      return true;
    }
    if (Schemer.isRequiredField(schema, fieldName)) {
      this.callbackOrThrow(
        buildError(`model.${operation}.unsetrequired`, fieldName),
        callback,
      );
      return true;
    }
    return false;
  }

  static getInplaceUpdateExpression(
    schema,
    fieldName,
    fieldValue,
    updateClauses,
    queryParams,
  ) {
    const $add = (_.isPlainObject(fieldValue) && fieldValue.$add) || false;
    const $append =
      (_.isPlainObject(fieldValue) && fieldValue.$append) || false;
    const $prepend =
      (_.isPlainObject(fieldValue) && fieldValue.$prepend) || false;
    const $replace =
      (_.isPlainObject(fieldValue) && fieldValue.$replace) || false;
    const $remove =
      (_.isPlainObject(fieldValue) && fieldValue.$remove) || false;

    fieldValue =
      $add || $append || $prepend || $replace || $remove || fieldValue;

    const dbVal = this.getDbValueExpression(schema, fieldName, fieldValue);

    if (!_.isPlainObject(dbVal) || !dbVal.query_segment) {
      updateClauses.push(util.format('"%s"=%s', fieldName, dbVal));
      return;
    }

    const fieldType = Schemer.getFieldType(schema, fieldName);

    if (['map', 'list', 'set'].includes(fieldType)) {
      if ($add || $append) {
        dbVal.query_segment = util.format(
          '"%s" + %s',
          fieldName,
          dbVal.query_segment,
        );
      } else if ($prepend) {
        if (fieldType === 'list') {
          dbVal.query_segment = util.format(
            '%s + "%s"',
            dbVal.query_segment,
            fieldName,
          );
        } else {
          throw buildError(
            'model.update.invalidprependop',
            util.format(
              '%s datatypes does not support $prepend, use $add instead',
              fieldType,
            ),
          );
        }
      } else if ($remove) {
        dbVal.query_segment = util.format(
          '"%s" - %s',
          fieldName,
          dbVal.query_segment,
        );
        if (fieldType === 'map') dbVal.parameter = Object.keys(dbVal.parameter);
      }
    }

    if ($replace) {
      if (fieldType === 'map') {
        updateClauses.push(
          util.format('"%s"[?]=%s', fieldName, dbVal.query_segment),
        );
        const replaceKeys = Object.keys(dbVal.parameter);
        const replaceValues = _.values(dbVal.parameter);
        if (replaceKeys.length === 1) {
          queryParams.push(replaceKeys[0]);
          queryParams.push(replaceValues[0]);
        } else {
          throw buildError(
            'model.update.invalidreplaceop',
            '$replace in map does not support more than one item',
          );
        }
      } else if (fieldType === 'list') {
        updateClauses.push(
          util.format('"%s"[?]=%s', fieldName, dbVal.query_segment),
        );
        if (dbVal.parameter.length === 2) {
          queryParams.push(dbVal.parameter[0]);
          queryParams.push(dbVal.parameter[1]);
        } else {
          throw buildError(
            'model.update.invalidreplaceop',
            '$replace in list should have exactly 2 items, first one as the index and the second one as the value',
          );
        }
      } else {
        throw buildError(
          'model.update.invalidreplaceop',
          util.format('%s datatypes does not support $replace', fieldType),
        );
      }
    } else {
      updateClauses.push(
        util.format('"%s"=%s', fieldName, dbVal.query_segment),
      );
      queryParams.push(dbVal.parameter);
    }
  }

  static getUpdateValueExpression(instance, schema, updateValues, callback) {
    const updateClauses = [];
    const queryParams = [];

    if (schema.options && schema.options.timestamps) {
      if (!updateValues[schema.options.timestamps.updatedAt]) {
        updateValues[schema.options.timestamps.updatedAt] = {
          $db_function: 'toTimestamp(now())',
        };
      }
    }

    if (schema.options && schema.options.versions) {
      if (!updateValues[schema.options.versions.key]) {
        updateValues[schema.options.versions.key] = { $db_function: 'now()' };
      }
    }

    const errorHappened = Object.keys(updateValues).some((fieldName) => {
      if (
        schema.fields[fieldName] === undefined ||
        schema.fields[fieldName].virtual
      )
        return false;

      const fieldType = Schemer.getFieldType(schema, fieldName);
      let fieldValue = updateValues[fieldName];

      if (fieldValue === undefined) {
        fieldValue = instance._get_default_value(fieldName);
        if (fieldValue === undefined) {
          return this.unsetNotAllowed('update', schema, fieldName, callback);
        } else if (
          !schema.fields[fieldName].rule ||
          !schema.fields[fieldName].rule.ignore_default
        ) {
          // did set a default value, ignore default is not set
          if (instance.validate(fieldName, fieldValue) !== true) {
            this.callbackOrThrow(
              buildError(
                'model.update.invaliddefaultvalue',
                fieldValue,
                fieldName,
                fieldType,
              ),
              callback,
            );
            return true;
          }
        }
      }

      if (fieldValue === null || fieldValue === cql.types.unset) {
        if (this.unsetNotAllowed('update', schema, fieldName, callback)) {
          return true;
        }
      }

      try {
        this.getInplaceUpdateExpression(
          schema,
          fieldName,
          fieldValue,
          updateClauses,
          queryParams,
        );
      } catch (e) {
        this.callbackOrThrow(e, callback);
        return true;
      }
      return false;
    });

    return { updateClauses, queryParams, errorHappened };
  }

  static getSaveValueExpression(instance, schema, callback) {
    const identifiers = [];
    const values = [];
    const queryParams = [];

    if (schema.options && schema.options.timestamps) {
      if (instance[schema.options.timestamps.updatedAt]) {
        instance[schema.options.timestamps.updatedAt] = {
          $db_function: 'toTimestamp(now())',
        };
      }
    }

    if (schema.options && schema.options.versions) {
      if (instance[schema.options.versions.key]) {
        instance[schema.options.versions.key] = { $db_function: 'now()' };
      }
    }

    const errorHappened = Object.keys(schema.fields).some((fieldName) => {
      if (schema.fields[fieldName].virtual) return false;

      // check field value
      const fieldType = Schemer.getFieldType(schema, fieldName);
      let fieldValue = instance[fieldName];

      if (fieldValue === undefined) {
        fieldValue = instance._get_default_value(fieldName);
        if (fieldValue === undefined) {
          return this.unsetNotAllowed('save', schema, fieldName, callback);
        } else if (
          !schema.fields[fieldName].rule ||
          !schema.fields[fieldName].rule.ignore_default
        ) {
          // did set a default value, ignore default is not set
          if (instance.validate(fieldName, fieldValue) !== true) {
            this.callbackOrThrow(
              buildError(
                'model.save.invaliddefaultvalue',
                fieldValue,
                fieldName,
                fieldType,
              ),
              callback,
            );
            return true;
          }
        }
      }

      if (fieldValue === null || fieldValue === cql.types.unset) {
        if (this.unsetNotAllowed('save', schema, fieldName, callback)) {
          return true;
        }
      }

      identifiers.push(util.format('"%s"', fieldName));

      try {
        const dbVal = this.getDbValueExpression(schema, fieldName, fieldValue);
        if (_.isPlainObject(dbVal) && dbVal.query_segment) {
          values.push(dbVal.query_segment);
          queryParams.push(dbVal.parameter);
        } else {
          values.push(dbVal);
        }
      } catch (e) {
        this.callbackOrThrow(e, callback);
        return true;
      }
      return false;
    });

    return {
      identifiers,
      values,
      queryParams,
      errorHappened,
    };
  }

  static extractQueryRelations(
    fieldName,
    relationKey,
    relationValue,
    schema,
    validOperators,
  ) {
    const queryRelations = [];
    const queryParams = [];

    if (!_.has(validOperators, relationKey.toLowerCase())) {
      throw buildError('model.find.invalidop', relationKey);
    }

    relationKey = relationKey.toLowerCase();
    if (relationKey === '$in' && !_.isArray(relationValue)) {
      throw buildError('model.find.invalidinop');
    }
    if (relationKey === '$token' && !(relationValue instanceof Object)) {
      throw buildError('model.find.invalidtoken');
    }

    let operator = validOperators[relationKey];
    let whereTemplate = '"%s" %s %s';

    const buildQueryRelations = (fieldNameLocal, relationValueLocal) => {
      const dbVal = this.getDbValueExpression(
        schema,
        fieldNameLocal,
        relationValueLocal,
      );
      if (_.isPlainObject(dbVal) && dbVal.query_segment) {
        queryRelations.push(
          util.format(
            whereTemplate,
            fieldNameLocal,
            operator,
            dbVal.query_segment,
          ),
        );
        queryParams.push(dbVal.parameter);
      } else {
        queryRelations.push(
          util.format(whereTemplate, fieldNameLocal, operator, dbVal),
        );
      }
    };

    const buildTokenQueryRelations = (tokenRelationKey, tokenRelationValue) => {
      tokenRelationKey = tokenRelationKey.toLowerCase();
      if (
        _.has(validOperators, tokenRelationKey) &&
        tokenRelationKey !== '$token' &&
        tokenRelationKey !== '$in'
      ) {
        operator = validOperators[tokenRelationKey];
      } else {
        throw buildError('model.find.invalidtokenop', tokenRelationKey);
      }

      if (_.isArray(tokenRelationValue)) {
        const tokenKeys = fieldName.split(',');
        for (
          let tokenIndex = 0;
          tokenIndex < tokenRelationValue.length;
          tokenIndex++
        ) {
          tokenKeys[tokenIndex] = tokenKeys[tokenIndex].trim();
          const dbVal = this.getDbValueExpression(
            schema,
            tokenKeys[tokenIndex],
            tokenRelationValue[tokenIndex],
          );
          if (_.isPlainObject(dbVal) && dbVal.query_segment) {
            tokenRelationValue[tokenIndex] = dbVal.query_segment;
            queryParams.push(dbVal.parameter);
          } else {
            tokenRelationValue[tokenIndex] = dbVal;
          }
        }
        queryRelations.push(
          util.format(
            whereTemplate,
            tokenKeys.join('","'),
            operator,
            tokenRelationValue.toString(),
          ),
        );
      } else {
        buildQueryRelations(fieldName, tokenRelationValue);
      }
    };

    if (relationKey === '$token') {
      whereTemplate = 'token("%s") %s token(%s)';

      const tokenRelationKeys = Object.keys(relationValue);
      for (let tokenRK = 0; tokenRK < tokenRelationKeys.length; tokenRK++) {
        const tokenRelationKey = tokenRelationKeys[tokenRK];
        const tokenRelationValue = relationValue[tokenRelationKey];
        buildTokenQueryRelations(tokenRelationKey, tokenRelationValue);
      }
    } else if (relationKey === '$contains') {
      const fieldType1 = Schemer.getFieldType(schema, fieldName);
      if (['map', 'list', 'set', 'frozen'].includes(fieldType1)) {
        if (fieldType1 === 'map' && _.isPlainObject(relationValue)) {
          Object.keys(relationValue).forEach((key) => {
            queryRelations.push(
              util.format('"%s"[%s] %s %s', fieldName, '?', '=', '?'),
            );
            queryParams.push(key);
            queryParams.push(relationValue[key]);
          });
        } else {
          queryRelations.push(
            util.format(whereTemplate, fieldName, operator, '?'),
          );
          queryParams.push(relationValue);
        }
      } else {
        throw buildError('model.find.invalidcontainsop');
      }
    } else if (relationKey === '$contains_key') {
      const fieldType2 = Schemer.getFieldType(schema, fieldName);
      if (fieldType2 !== 'map') {
        throw buildError('model.find.invalidcontainskeyop');
      }
      queryRelations.push(util.format(whereTemplate, fieldName, operator, '?'));
      queryParams.push(relationValue);
    } else {
      buildQueryRelations(fieldName, relationValue);
    }
    return { queryRelations, queryParams };
  }

  static _parseQueryObject(schema, queryObject) {
    let queryRelations = [];
    let queryParams = [];

    Object.keys(queryObject).forEach((fieldName) => {
      if (fieldName.startsWith('$')) {
        // search queries based on lucene index or solr
        // escape all single quotes for queries in cassandra
        if (fieldName === '$expr') {
          if (
            typeof queryObject[fieldName].index === 'string' &&
            typeof queryObject[fieldName].query === 'string'
          ) {
            queryRelations.push(
              util.format(
                "expr(%s,'%s')",
                queryObject[fieldName].index,
                queryObject[fieldName].query.replace(/'/g, "''"),
              ),
            );
          } else {
            throw buildError('model.find.invalidexpr');
          }
        } else if (fieldName === '$solr_query') {
          if (typeof queryObject[fieldName] === 'string') {
            queryRelations.push(
              util.format(
                "solr_query='%s'",
                queryObject[fieldName].replace(/'/g, "''"),
              ),
            );
          } else {
            throw buildError('model.find.invalidsolrquery');
          }
        }
        return;
      }

      let whereObject = queryObject[fieldName];
      // Array of operators
      if (!_.isArray(whereObject)) whereObject = [whereObject];

      for (let fk = 0; fk < whereObject.length; fk++) {
        let fieldRelation = whereObject[fk];

        const cqlOperators = {
          $eq: '=',
          $ne: '!=',
          $isnt: 'IS NOT',
          $gt: '>',
          $lt: '<',
          $gte: '>=',
          $lte: '<=',
          $in: 'IN',
          $like: 'LIKE',
          $token: 'token',
          $contains: 'CONTAINS',
          $contains_key: 'CONTAINS KEY',
        };

        if (_.isPlainObject(fieldRelation)) {
          const validKeys = Object.keys(cqlOperators);
          const fieldRelationKeys = Object.keys(fieldRelation);
          for (let i = 0; i < fieldRelationKeys.length; i++) {
            if (!validKeys.includes(fieldRelationKeys[i])) {
              // field relation key invalid, apply default $eq operator
              fieldRelation = { $eq: fieldRelation };
              break;
            }
          }
        } else {
          fieldRelation = { $eq: fieldRelation };
        }

        const relationKeys = Object.keys(fieldRelation);
        for (let rk = 0; rk < relationKeys.length; rk++) {
          const relationKey = relationKeys[rk];
          const relationValue = fieldRelation[relationKey];
          const extractedRelations = this.extractQueryRelations(
            fieldName,
            relationKey,
            relationValue,
            schema,
            cqlOperators,
          );
          queryRelations = queryRelations.concat(
            extractedRelations.queryRelations,
          );
          queryParams = queryParams.concat(extractedRelations.queryParams);
        }
      }
    });

    return { queryRelations, queryParams };
  }

  static getFilterClause(schema, queryObject, clause) {
    const parsedObject = this._parseQueryObject(schema, queryObject);
    const filterClause: any = {};
    if (parsedObject.queryRelations.length > 0) {
      filterClause.query = util.format(
        '%s %s',
        clause,
        parsedObject.queryRelations.join(' AND '),
      );
    } else {
      filterClause.query = '';
    }
    filterClause.params = parsedObject.queryParams;
    return filterClause;
  }

  static getFilterClauseDdl(schema, queryObject, clause) {
    const filterClause = this.getFilterClause(schema, queryObject, clause);
    let filterQuery = filterClause.query;
    filterClause.params.forEach((param) => {
      let queryParam;
      if (typeof param === 'string') {
        queryParam = util.format("'%s'", param);
      } else if (param instanceof Date) {
        queryParam = util.format("'%s'", param.toISOString());
      } else if (
        param instanceof cql.types.Long ||
        param instanceof cql.types.Integer ||
        param instanceof cql.types.BigDecimal ||
        param instanceof cql.types.TimeUuid ||
        param instanceof cql.types.Uuid
      ) {
        queryParam = param.toString();
      } else if (
        param instanceof cql.types.LocalDate ||
        param instanceof cql.types.LocalTime ||
        param instanceof cql.types.InetAddress
      ) {
        queryParam = util.format("'%s'", param.toString());
      } else {
        queryParam = param;
      }
      // TODO: unhandled if queryParam is a string containing ? character
      // though this is unlikely to have in materialized view filters, but...
      filterQuery = filterQuery.replace('?', queryParam);
    });
    return filterQuery;
  }

  static getWhereClause(schema, queryObject) {
    return this.getFilterClause(schema, queryObject, 'WHERE');
  }

  static getIfClause(schema, queryObject) {
    return this.getFilterClause(schema, queryObject, 'IF');
  }

  static getPrimaryKeyClauses(schema) {
    const partitionKey = schema.key[0];
    let clusteringKey = schema.key.slice(1, schema.key.length);
    const clusteringOrder = [];

    for (let field = 0; field < clusteringKey.length; field++) {
      if (
        schema.clustering_order &&
        schema.clustering_order[clusteringKey[field]] &&
        schema.clustering_order[clusteringKey[field]].toLowerCase() === 'desc'
      ) {
        clusteringOrder.push(util.format('"%s" DESC', clusteringKey[field]));
      } else {
        clusteringOrder.push(util.format('"%s" ASC', clusteringKey[field]));
      }
    }

    let clusteringOrderClause = '';
    if (clusteringOrder.length > 0) {
      clusteringOrderClause = util.format(
        ' WITH CLUSTERING ORDER BY (%s)',
        clusteringOrder.toString(),
      );
    }

    let partitionKeyClause = '';
    if (_.isArray(partitionKey)) {
      partitionKeyClause = partitionKey
        .map((v) => util.format('"%s"', v))
        .join(',');
    } else {
      partitionKeyClause = util.format('"%s"', partitionKey);
    }

    let clusteringKeyClause = '';
    if (clusteringKey.length) {
      clusteringKey = clusteringKey
        .map((v) => util.format('"%s"', v))
        .join(',');
      clusteringKeyClause = util.format(',%s', clusteringKey);
    }

    return { partitionKeyClause, clusteringKeyClause, clusteringOrderClause };
  }

  static getMviewWhereClause(schema, viewSchema) {
    const clauses = this.getPrimaryKeyClauses(viewSchema);
    let whereClause = clauses.partitionKeyClause
      .split(',')
      .join(' IS NOT NULL AND ');
    if (clauses.clusteringKeyClause)
      whereClause += clauses.clusteringKeyClause
        .split(',')
        .join(' IS NOT NULL AND ');
    whereClause += ' IS NOT NULL';

    const filters = _.cloneDeep(viewSchema.filters);

    if (_.isPlainObject(filters)) {
      // delete primary key fields defined as isn't null in filters
      Object.keys(filters).forEach((filterKey) => {
        if (
          filters[filterKey].$isnt === null &&
          (viewSchema.key.includes(filterKey) ||
            viewSchema.key[0].includes(filterKey))
        ) {
          delete filters[filterKey].$isnt;
        }
      });

      const filterClause = this.getFilterClauseDdl(schema, filters, 'AND');
      whereClause += util
        .format(' %s', filterClause)
        .replace(/IS NOT null/g, 'IS NOT NULL');
    }

    // remove unnecessarily quoted field names in generated where clause
    // so that it matches the where_clause from database schema
    const quotedFieldNames = whereClause.match(/"(.*?)"/g);
    quotedFieldNames.forEach((fieldName) => {
      const unquotedFieldName = fieldName.replace(/"/g, '');
      const reservedKeywords = [
        'ADD',
        'AGGREGATE',
        'ALLOW',
        'ALTER',
        'AND',
        'ANY',
        'APPLY',
        'ASC',
        'AUTHORIZE',
        'BATCH',
        'BEGIN',
        'BY',
        'COLUMNFAMILY',
        'CREATE',
        'DELETE',
        'DESC',
        'DROP',
        'EACH_QUORUM',
        'ENTRIES',
        'FROM',
        'FULL',
        'GRANT',
        'IF',
        'IN',
        'INDEX',
        'INET',
        'INFINITY',
        'INSERT',
        'INTO',
        'KEYSPACE',
        'KEYSPACES',
        'LIMIT',
        'LOCAL_ONE',
        'LOCAL_QUORUM',
        'MATERIALIZED',
        'MODIFY',
        'NAN',
        'NORECURSIVE',
        'NOT',
        'OF',
        'ON',
        'ONE',
        'ORDER',
        'PARTITION',
        'PASSWORD',
        'PER',
        'PRIMARY',
        'QUORUM',
        'RENAME',
        'REVOKE',
        'SCHEMA',
        'SELECT',
        'SET',
        'TABLE',
        'TIME',
        'THREE',
        'TO',
        'TOKEN',
        'TRUNCATE',
        'TWO',
        'UNLOGGED',
        'UPDATE',
        'USE',
        'USING',
        'VIEW',
        'WHERE',
        'WITH',
      ];
      if (
        unquotedFieldName === unquotedFieldName.toLowerCase() &&
        !reservedKeywords.includes(unquotedFieldName.toUpperCase())
      ) {
        whereClause = whereClause.replace(fieldName, unquotedFieldName);
      }
    });
    return whereClause.trim();
  }

  static getOrderByClause(queryObject) {
    const orderKeys = [];
    Object.keys(queryObject).forEach((k) => {
      const queryItem = queryObject[k];
      if (k.toLowerCase() === '$orderby') {
        if (!(queryItem instanceof Object)) {
          throw buildError('model.find.invalidorder');
        }
        const orderItemKeys = Object.keys(queryItem);

        for (let i = 0; i < orderItemKeys.length; i++) {
          const cqlOrderDirection = { $asc: 'ASC', $desc: 'DESC' };
          if (orderItemKeys[i].toLowerCase() in cqlOrderDirection) {
            let orderFields = queryItem[orderItemKeys[i]];

            if (!_.isArray(orderFields)) {
              orderFields = [orderFields];
            }

            for (let j = 0; j < orderFields.length; j++) {
              orderKeys.push(
                util.format(
                  '"%s" %s',
                  orderFields[j],
                  cqlOrderDirection[orderItemKeys[i]],
                ),
              );
            }
          } else {
            throw buildError('model.find.invalidordertype', orderItemKeys[i]);
          }
        }
      }
    });
    return orderKeys.length
      ? util.format('ORDER BY %s', orderKeys.join(', '))
      : '';
  }

  static getGroupByClause(queryObject) {
    let groupByKeys = [];

    Object.keys(queryObject).forEach((k) => {
      const queryItem = queryObject[k];

      if (k.toLowerCase() === '$groupby') {
        if (!(queryItem instanceof Array)) {
          throw buildError('model.find.invalidgroup');
        }

        groupByKeys = groupByKeys.concat(queryItem);
      }
    });

    groupByKeys = groupByKeys.map((key) => `"${key}"`);

    return groupByKeys.length
      ? util.format('GROUP BY %s', groupByKeys.join(', '))
      : '';
  }

  static getLimitClause(queryObject) {
    let limitClause = '';
    Object.keys(queryObject).forEach((k) => {
      const queryItem = queryObject[k];
      if (
        k.toLowerCase() === '$limit' ||
        k.toLowerCase() === '$per_partition_limit'
      ) {
        if (typeof queryItem !== 'number')
          throw buildError('model.find.limittype');
        limitClause = util.format('LIMIT %s', queryItem);
      }
      if (k.toLowerCase() === '$per_partition_limit') {
        limitClause = util.format('PER PARTITION %s', limitClause);
      }
    });
    return limitClause;
  }

  static getSelectClause(options) {
    let selectClause = '*';
    if (
      options.select &&
      _.isArray(options.select) &&
      options.select.length > 0
    ) {
      const selectArray = [];
      for (let i = 0; i < options.select.length; i++) {
        // separate the aggregand the column name if select is an aggrega
        const selection = options.select[i].split(/[(, )]/g).filter((e) => e);
        if (selection.length === 1) {
          if (selection[0] === '*') selectArray.push('*');
          else selectArray.push(util.format('"%s"', selection[0]));
        } else if (selection.length === 2) {
          selectArray.push(util.format('%s("%s")', selection[0], selection[1]));
        } else if (
          selection.length >= 3 &&
          selection[selection.length - 2].toLowerCase() === 'as'
        ) {
          const selectionEndChunk = selection.splice(selection.length - 2);
          let selectionChunk = '';
          if (selection.length === 1) {
            selectionChunk = util.format('"%s"', selection[0]);
          } else if (selection.length === 2) {
            selectionChunk = util.format(
              '%s("%s")',
              selection[0],
              selection[1],
            );
          } else {
            selectionChunk = util.format(
              '%s(%s)',
              selection[0],
              `"${selection.splice(1).join('","')}"`,
            );
          }
          selectArray.push(
            util.format('%s AS "%s"', selectionChunk, selectionEndChunk[1]),
          );
        } else if (selection.length >= 3) {
          selectArray.push(
            util.format(
              '%s(%s)',
              selection[0],
              `"${selection.splice(1).join('","')}"`,
            ),
          );
        }
      }
      selectClause = selectArray.join(',');
    }
    return selectClause.trim();
  }
}
