package io.confluent.connect.hdfs.csv;

import io.confluent.connect.storage.hive.HiveFactory;
import io.confluent.connect.storage.hive.HiveUtil;
import org.apache.kafka.common.config.AbstractConfig;

public class CsvHiveFactory implements HiveFactory {

  @Override
  public HiveUtil createHiveUtil(
      AbstractConfig config, io.confluent.connect.storage.hive.HiveMetaStore hiveMetaStore) {
    return createHiveUtil(config, hiveMetaStore);
  }
}
