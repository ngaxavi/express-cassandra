const Promise = require('bluebird');
const util = require('util');
const check = require('check-types');
import * as cassandraDriver from 'cassandra-driver';

let dseDriver;
try {
  // eslint-disable-next-line import/no-extraneous-dependencies, import/no-unresolved
  dseDriver = require('dse-driver');
} catch (e) {
  dseDriver = null;
}

const cql = Promise.promisifyAll(dseDriver || cassandraDriver);

const validators: any = {};
validators.isArray = (obj) => (check.array(obj));
validators.isObject = (obj) => (check.object(obj));
validators.isLong = (obj) => ((obj instanceof cql.types.Long));
validators.isDecimal = (obj) => ((obj instanceof cql.types.BigDecimal));
validators.isInteger = (obj) => (check.integer(obj));
validators.isVarInt = (obj) => ((obj instanceof cql.types.Integer));
validators.isBoolean = (obj) => ((obj === true || obj === false));
validators.isNumber = (obj) => (check.number(obj));
validators.isString = (obj) => (check.string(obj));
validators.isLocalDate = (obj) => ((obj instanceof cql.types.LocalDate));
validators.isLocalTime = (obj) => ((obj instanceof cql.types.LocalTime));
validators.isDate = (obj) => (check.date(new Date(obj)));
validators.isBuffer = (obj) => ((obj instanceof Buffer));
validators.isTimeUuid = (obj) => ((obj instanceof cql.types.TimeUuid));
validators.isUuid = (obj) => ((obj instanceof cql.types.Uuid));
validators.isInet = (obj) => ((obj instanceof cql.types.InetAddress));
validators.isFrozen = (obj) => ((validators.is_array(obj) || validators.is_object(obj)));

export const TYPE_MAP = {
  ascii: { validator: validators.isString },
  bigint: { validator: validators.isLong },
  blob: { validator: validators.isBuffer },
  boolean: { validator: validators.isBoolean },
  counter: { validator: validators.isLong },
  date: { validator: validators.isLocalDate },
  decimal: { validator: validators.isDecimal },
  double: { validator: validators.isNumber },
  float: { validator: validators.isNumber },
  inet: { validator: validators.isInet },
  int: { validator: validators.isInteger },
  list: { validator: validators.isArray },
  map: { validator: validators.isObject },
  set: { validator: validators.isArray },
  smallint: { validator: validators.isInteger },
  text: { validator: validators.isString },
  time: { validator: validators.isLocalTime },
  timestamp: { validator: validators.isDate },
  timeuuid: { validator: validators.isTimeUuid },
  tinyint: { validator: validators.isInteger },
  uuid: { validator: validators.isUuid },
  varchar: { validator: validators.isString },
  varint: { validator: validators.isVarInt },
  frozen: { validator: validators.isFrozen },
  genericTypeValidator: (fieldType) => {
    if (!TYPE_MAP[fieldType]) return null;
    return {
      validator: TYPE_MAP[fieldType].validator,
      message(value, propName, fieldType) {
        return util.format('Invalid Value: "%s" for Field: %s (Type: %s)', value, propName, fieldType);
      },
      type: 'type_validator',
    };
  }
};

