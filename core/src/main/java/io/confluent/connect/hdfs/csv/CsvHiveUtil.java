package io.confluent.connect.hdfs.csv;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.hive.HiveMetaStore;
import io.confluent.connect.hdfs.hive.HiveUtil;
import io.confluent.connect.hdfs.partitioner.Partitioner;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.errors.HiveMetaStoreException;
import io.confluent.connect.storage.hive.HiveSchemaConverter;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.kafka.connect.data.Schema;

public class CsvHiveUtil extends HiveUtil {

  private final String topicsDir;
  private static final String OPENCSV_SERDE = "org.apache.hadoop.hive.serde2.OpenCSVSerde";
  private static final String TEXT_INPUT_FORMAT = "org.apache.hadoop.mapred.TextInputFormat";
  private static final String TEXT_OUTPU_TFORMAT = "org.apache.hadoop.mapred.TextOutputFormat";

  public CsvHiveUtil(HdfsSinkConnectorConfig conf, HiveMetaStore hiveMetaStore) {
    super(conf, hiveMetaStore);
    this.topicsDir = conf.getString(StorageCommonConfig.TOPICS_DIR_CONFIG);
  }

  @Override
  public void createTable(
      String database, String tableName, Schema schema, Partitioner partitioner) {
    Table table = constructCsvTable(database, tableName, schema, partitioner);
    hiveMetaStore.createTable(table);
  }

  private Table constructCsvTable(
      String database, String tableName, Schema schema, Partitioner partitioner)
      throws HiveMetaStoreException {
    Table table = newTable(database, tableName);
    table.setTableType(TableType.EXTERNAL_TABLE);
    table.getParameters().put("EXTERNAL", "TRUE");
    String tablePath = hiveDirectoryName(url, topicsDir, tableName);
    table.setDataLocation(new Path(tablePath));
    table.setSerializationLib(OPENCSV_SERDE);
    try {
      table.setInputFormatClass(TEXT_INPUT_FORMAT);
      table.setOutputFormatClass(TEXT_OUTPU_TFORMAT);
    } catch (HiveException e) {
      throw new HiveMetaStoreException("Cannot find input/output format:", e);
    }
    List<FieldSchema> columns = HiveSchemaConverter.convertSchema(schema);
    table.setFields(columns);
    if (partitioner != null) {
      table.setPartCols(partitioner.partitionFields());
    }
    return table;
  }
}
