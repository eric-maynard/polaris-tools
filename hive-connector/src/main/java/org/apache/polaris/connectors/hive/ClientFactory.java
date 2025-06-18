package org.apache.polaris.connectors.hive;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;

public class ClientFactory extends BasePooledObjectFactory<IMetaStoreClient> {

  private final HiveConf hiveConf;

  public ClientFactory(String metastoreUri) {
    hiveConf = new HiveConf();
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, metastoreUri);
  }

  @Override
  public IMetaStoreClient create() throws Exception {
    return RetryingMetaStoreClient.getProxy(hiveConf, true);
  }

  @Override
  public PooledObject<IMetaStoreClient> wrap(IMetaStoreClient client) {
    return new DefaultPooledObject<>(client);
  }

  @Override
  public void destroyObject(PooledObject<IMetaStoreClient> p) throws Exception {
    p.getObject().close();
    super.destroyObject(p);
  }
}