const _ = require('lodash');
const debug = require('debug')('express-cassandra');

export class JanusGraphBuilder {
  constructor(private readonly _client, private readonly _config: any = {}) {}

  createGraph(graphName, callback) {
    debug('creating janus graph: %s', graphName);
    const script = `
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("storage.backend", storageBackend);
      map.put("storage.hostname", storageHostname);
      map.put("storage.port", storagePort);
      map.put("index.search.backend", indexBackend);
      map.put("index.search.hostname", indexHostname);
      map.put("index.search.port", indexPort);
      map.put("graph.graphname", graphName);
      ConfiguredGraphFactory.createConfiguration(new MapConfiguration(map));
      ConfiguredGraphFactory.open(graphName).vertices().size();
    `;
    const bindings = {
      storageBackend: this._config.storage.backend,
      storageHostname: this._config.storage.hostname,
      storagePort: this._config.storage.port,
      indexBackend: this._config.index.search.backend,
      indexHostname: this._config.index.search.hostname,
      indexPort: this._config.index.search.port,
      graphName,
    };
    this._client.execute(script, bindings, (err, results) => {
      if (err) {
        callback(err);
        return;
      }

      callback(null, results);
    });
  }

  checkGraphExist(graphName, callback) {
    debug('check for janus graph: %s', graphName);
    const script = `
      ConfiguredGraphFactory.getGraphNames();
    `;
    const bindings = {};
    this._client.execute(script, bindings, (err, results) => {
      if (err) {
        callback(err);
        return;
      }

      if (_.isArray(results) && results.includes(graphName)) {
        callback(null, true);
        return;
      }
      callback(null, false);
    });
  }

  assertGraph(graphName, callback) {
    this.checkGraphExist(graphName, (err, exist) => {
      if (err) {
        callback(err);
        return;
      }

      if (!exist) {
        this.createGraph(graphName, callback);
        return;
      }

      callback();
    });
  }

  dropGraph(graphName, callback) {
    debug('removing janus graph: %s', graphName);
    const script = `
      ConfiguredGraphFactory.drop(graphName);
    `;
    const bindings = {
      graphName,
    };
    this._client.execute(script, bindings, (err, results) => {
      if (err) {
        callback(err);
        return;
      }

      callback(null, results);
    });
  }

  putIndexes(graphName, mappingName, indexes, callback) {
    debug('syncing janus graph indexes for: %s', mappingName);
    let script = `
      graph = ConfiguredGraphFactory.open(graphName);
      graph.tx().commit();
      mgmt = graph.openManagement();
    `;
    const bindings = {
      graphName,
    };
    // create indexes if not exist
    Object.keys(indexes).forEach((index) => {
      if (indexes[index].type === 'Composite') {
        script += `if (!mgmt.containsGraphIndex('${index}')) mgmt.buildIndex('${index}', Vertex.class)`;
        indexes[index].keys.forEach((key) => {
          script += `.addKey(mgmt.getPropertyKey('${key}'))`;
        });
        script += `.indexOnly(mgmt.getVertexLabel('${mappingName}'))`;
        if (indexes[index].unique) {
          script += '.unique()';
        }
        script += '.buildCompositeIndex();';
      } else if (indexes[index].type === 'Mixed') {
        script += `if (!mgmt.containsGraphIndex('${index}')) mgmt.buildIndex('${index}', Vertex.class)`;
        indexes[index].keys.forEach((key) => {
          script += `.addKey(mgmt.getPropertyKey('${key}'))`;
        });
        script += `.indexOnly(mgmt.getVertexLabel('${mappingName}'))`;
        if (indexes[index].unique) {
          script += '.unique()';
        }
        script += '.buildMixedIndex("search");';
      } else if (indexes[index].type === 'VertexCentric') {
        script += `relationLabel = mgmt.getEdgeLabel('${indexes[index].label}');`;
        script += `if (!mgmt.containsRelationIndex(relationLabel, '${index}')) mgmt.buildEdgeIndex(relationLabel, '${index}', Direction.${indexes[index].direction}, Order.${indexes[index].order}`;
        indexes[index].keys.forEach((key) => {
          script += `, mgmt.getPropertyKey('${key}')`;
        });
        script += ');';
      }
    });
    script += 'mgmt.commit();';
    // await index for registered or enabled status
    Object.keys(indexes).forEach((index) => {
      if (indexes[index].type === 'Composite') {
        script += `mgmt.awaitGraphIndexStatus(graph, '${index}').status(SchemaStatus.REGISTERED, SchemaStatus.ENABLED).call();`;
      } else if (indexes[index].type === 'Mixed') {
        script += `mgmt.awaitGraphIndexStatus(graph, '${index}').status(SchemaStatus.REGISTERED, SchemaStatus.ENABLED).call();`;
      } else if (indexes[index].type === 'VertexCentric') {
        script += `mgmt.awaitRelationIndexStatus(graph, '${index}', '${indexes[index].label}').status(SchemaStatus.REGISTERED, SchemaStatus.ENABLED).call();`;
      }
    });
    // enable index if in registered state
    script += 'mgmt = graph.openManagement();';
    Object.keys(indexes).forEach((index) => {
      if (indexes[index].type === 'Composite') {
        script += `if (mgmt.getGraphIndex('${index}').getIndexStatus(mgmt.getPropertyKey('${indexes[index].keys[0]}')).equals(SchemaStatus.REGISTERED)) mgmt.updateIndex(mgmt.getGraphIndex('${index}'), SchemaAction.ENABLE_INDEX);`;
      } else if (indexes[index].type === 'Mixed') {
        script += `if (mgmt.getGraphIndex('${index}').getIndexStatus(mgmt.getPropertyKey('${indexes[index].keys[0]}')).equals(SchemaStatus.REGISTERED)) mgmt.updateIndex(mgmt.getGraphIndex('${index}'), SchemaAction.ENABLE_INDEX);`;
      } else if (indexes[index].type === 'VertexCentric') {
        script += `if (mgmt.getRelationIndex(mgmt.getEdgeLabel('${indexes[index].label}'), '${index}').getIndexStatus().equals(SchemaStatus.REGISTERED)) mgmt.updateIndex(mgmt.getRelationIndex(mgmt.getEdgeLabel('${indexes[index].label}'), '${index}'), SchemaAction.ENABLE_INDEX);`;
      }
    });
    script += 'mgmt.commit();';
    // await index for enabled status
    Object.keys(indexes).forEach((index) => {
      if (indexes[index].type === 'Composite') {
        script += `mgmt.awaitGraphIndexStatus(graph, '${index}').status(SchemaStatus.ENABLED).call();`;
      } else if (indexes[index].type === 'Mixed') {
        script += `mgmt.awaitGraphIndexStatus(graph, '${index}').status(SchemaStatus.ENABLED).call();`;
      } else if (indexes[index].type === 'VertexCentric') {
        script += `mgmt.awaitRelationIndexStatus(graph, '${index}', '${indexes[index].label}').status(SchemaStatus.ENABLED).call();`;
      }
    });
    this._client.execute(script, bindings, (err, results) => {
      if (err) {
        callback(err);
        return;
      }

      callback(null, results);
    });
  }

  putMapping(graphName, mappingName, mappingBody, callback) {
    debug('syncing janus graph mapping: %s', mappingName);
    let script = `
      graph = ConfiguredGraphFactory.open(graphName);
      graph.tx().commit();
      mgmt = graph.openManagement();
      if (!mgmt.containsVertexLabel(mappingName)) mgmt.makeVertexLabel(mappingName).make();
    `;
    const bindings = {
      graphName,
      mappingName,
    };
    Object.keys(mappingBody.relations).forEach((relation) => {
      script += `
        if (!mgmt.containsEdgeLabel('${relation}')) mgmt.makeEdgeLabel('${relation}').multiplicity(${mappingBody.relations[relation]}).make();
      `;
    });
    Object.keys(mappingBody.properties).forEach((property) => {
      script += `
        if (!mgmt.containsPropertyKey('${property}')) mgmt.makePropertyKey('${property}').dataType(${mappingBody.properties[property].type}.class).cardinality(Cardinality.${mappingBody.properties[property].cardinality}).make();
      `;
    });
    script += 'mgmt.commit();';
    this._client.execute(script, bindings, (err, results) => {
      if (err) {
        callback(err);
        return;
      }

      if (Object.keys(mappingBody.indexes).length > 0) {
        this.putIndexes(graphName, mappingName, mappingBody.indexes, callback);
        return;
      }

      callback(null, results);
    });
  }
}
