/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.polaris.connectors.hive;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.entity.table.GenericTableEntity;
import org.apache.polaris.service.catalog.generic.GenericTableCatalog;

import java.io.Closeable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implements `GenericTableCatalog` by delegating to a Hive Metastote instance
 */
public class HiveGenericCatalog implements GenericTableCatalog, Closeable {

  public static final String CLIENT_POOL_MAX_TOTAL = "polaris.config.client-pool-max-total";
  public static final String CLIENT_POOL_MAX_IDLE = "polaris.config.client-pool-max-idle";

  private static final String CLIENT_POOL_MAX_TOTAL_DEFAULT = "20";
  private static final String CLIENT_POOL_MAX_IDLE_DEFAULT = "5";

  private GenericObjectPool<IMetaStoreClient> clientPool;

  @Override
  public void initialize(String name, Map<String, String> properties) {
    // TODO once Polaris supports an auth mechanism with HMS, use the properties provided

    String uri = properties.get(CatalogProperties.URI);
    String clientPoolMaxTotalString =
      properties.getOrDefault(CLIENT_POOL_MAX_TOTAL, CLIENT_POOL_MAX_TOTAL_DEFAULT);
    int clientPoolMaxTotal = Integer.parseInt(clientPoolMaxTotalString);
    String clientPoolMaxIdleString =
      properties.getOrDefault(CLIENT_POOL_MAX_IDLE, CLIENT_POOL_MAX_IDLE_DEFAULT);
    int clientPoolMaxIdle = Integer.parseInt(clientPoolMaxIdleString);

    ClientFactory factory = new ClientFactory(uri);
    GenericObjectPoolConfig<IMetaStoreClient> config = new GenericObjectPoolConfig<>();
    config.setMaxTotal(clientPoolMaxTotal);
    config.setMaxIdle(clientPoolMaxIdle);
    config.setMinIdle(1);
    config.setBlockWhenExhausted(true);
    this.clientPool = new GenericObjectPool<>(factory, config);
  }

  private <T> T withClient(FunctionWithException<IMetaStoreClient, T> function) {
    IMetaStoreClient client = null;
    try {
      client = clientPool.borrowObject();
      return function.apply(client);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (client != null) {
        clientPool.returnObject(client);
      }
    }
  }

  @Override
  public List<TableIdentifier> listGenericTables(Namespace namespace) {
    return withClient(client -> {
      final List<String> tableNames;
      if (namespace.levels().length == 1) {
        tableNames = client.getAllTables(namespace.level(0));
      } else {
        throw new IllegalArgumentException("Hive namespaces only support one level");
      }
      return tableNames.stream().map(name -> {
        return TableIdentifier.of(namespace, name);
      }).toList();
    });
  }

  @Override
  public boolean dropGenericTable(TableIdentifier tableIdentifier) {
    return withClient(client -> {
      Namespace namespace = tableIdentifier.namespace();
      if (namespace.levels().length == 1) {
        client.dropTable(namespace.level(0), tableIdentifier.name());
      } else {
        throw new IllegalArgumentException("Hive namespaces only support one level");
      }
      return true;
    });
  }

  private GenericTableEntity tableToGenericTable(
      TableIdentifier tableIdentifier, Table table) {
    String format = FormatUtils.getTableFormat(table);

    // Polaris will need to re-wrap this with a legitimate catalog ID, parent ID, etc.
    return new GenericTableEntity.Builder(tableIdentifier, format)
      .setParentNamespace(tableIdentifier.namespace())
      .setProperties(table.getParameters())
      .setCreateTimestamp(table.getCreateTime())
      .build();
  }

  @Override
  public GenericTableEntity loadGenericTable(TableIdentifier tableIdentifier) {
    return withClient(client -> {
      Namespace namespace = tableIdentifier.namespace();
      Table table;
      if (namespace.levels().length == 1) {
        table = client.getTable(namespace.level(0), tableIdentifier.name());
      } else {
        throw new IllegalArgumentException("Hive namespaces only support one level");
      }
      return tableToGenericTable(tableIdentifier, table);
    });
  }

  @Override
  public GenericTableEntity createGenericTable(
      TableIdentifier tableIdentifier, String format, String doc, Map<String, String> properties) {
    return withClient(client -> {
      Namespace namespace = tableIdentifier.namespace();
      if (namespace.levels().length != 1) {
        throw new IllegalArgumentException("Hive namespaces only support one level");
      }

      Table table = new Table();
      table.setDbName(table.getDbName());
      table.setTableName(tableIdentifier.name());
      table.setCreateTime((int) System.currentTimeMillis()); // 2038 problem?

      HashMap<String, String> propertiesWithFormat = new HashMap<>(properties);
      propertiesWithFormat.put(FormatUtils.FORMAT_PROPERTY, format);
      table.setParameters(propertiesWithFormat);

      client.createTable(table);

      return tableToGenericTable(tableIdentifier, table);
    });
  }

  @Override
  public void close() {
    clientPool.close();
  }

  @FunctionalInterface
  interface FunctionWithException<T, R> {
    R apply(T t) throws Exception;
  }
}
