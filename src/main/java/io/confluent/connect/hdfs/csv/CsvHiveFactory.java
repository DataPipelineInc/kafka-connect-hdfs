package io.confluent.connect.hdfs.csv;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.hive.HiveMetaStore;
import io.confluent.connect.storage.hive.HiveFactory;
import io.confluent.connect.storage.hive.HiveUtil;
import org.apache.kafka.common.config.AbstractConfig;

public class CsvHiveFactory implements HiveFactory {

  @Override
  public HiveUtil createHiveUtil(
      AbstractConfig config, io.confluent.connect.storage.hive.HiveMetaStore hiveMetaStore) {
    return createHiveUtil((HdfsSinkConnectorConfig) config, (HiveMetaStore) hiveMetaStore);
  }

  public HiveUtil createHiveUtil(HdfsSinkConnectorConfig config, HiveMetaStore hiveMetaStore) {
    return new CsvHiveUtil(config, hiveMetaStore);
  }
}
