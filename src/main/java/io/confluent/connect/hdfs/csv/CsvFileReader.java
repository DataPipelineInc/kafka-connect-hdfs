package io.confluent.connect.hdfs.csv;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.csv.converter.CsvSchemaConverter;
import io.confluent.connect.hdfs.hive.HiveMetaStore;
import io.confluent.connect.storage.hive.HiveConfig;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;

public class CsvFileReader
    implements io.confluent.connect.storage.format.SchemaFileReader<HdfsSinkConnectorConfig, Path> {

  public static final String SCHEMA_NAME = "schema.name";

  @Override
  public Schema getSchema(HdfsSinkConnectorConfig conf, Path path) {
    if (conf.getBoolean(HiveConfig.HIVE_INTEGRATION_CONFIG)) {
      HiveMetaStore hiveMetaStore = new HiveMetaStore(conf.getHadoopConfiguration(), conf);
      String database = conf.getString(HiveConfig.HIVE_DATABASE_CONFIG);
      String tableName = conf.getString(SCHEMA_NAME);
      Table table = hiveMetaStore.getTable(database, tableName);
      List<FieldSchema> cols = table.getCols();
      SchemaBuilder schemaBuilder = new SchemaBuilder(Type.STRUCT);
      cols.forEach(
          col -> schemaBuilder.field(col.getName(), CsvSchemaConverter.getSchema(col.getType())));
      return schemaBuilder.build();
    }
    return null;
  }

  @Override
  public boolean hasNext() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object next() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<Object> iterator() {
        throw new UnsupportedOperationException();
    }

  @Override
  public void close() {}
}
