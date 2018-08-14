package io.confluent.connect.hdfs.csv;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.format.SchemaFileReader;
import io.confluent.connect.storage.hive.HiveFactory;
import org.apache.hadoop.fs.Path;

public class CsvFormat
    implements io.confluent.connect.storage.format.Format<HdfsSinkConnectorConfig, Path> {

  public CsvFormat(HdfsStorage storage) {}

  @Override
  public RecordWriterProvider<HdfsSinkConnectorConfig> getRecordWriterProvider() {
    return new CsvRecordWriterProvider();
  }

  @Override
  public SchemaFileReader<HdfsSinkConnectorConfig, Path> getSchemaFileReader() {
    return new CsvFileReader();
  }

  @Override
  public HiveFactory getHiveFactory() {
    return new CsvHiveFactory();
  }
}
