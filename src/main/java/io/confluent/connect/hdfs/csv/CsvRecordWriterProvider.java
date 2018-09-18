package io.confluent.connect.hdfs.csv;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.storage.format.RecordWriter;
import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONException;

public class CsvRecordWriterProvider
    implements io.confluent.connect.storage.format.RecordWriterProvider<HdfsSinkConnectorConfig> {

  public static final String FILE_DELIMITER = "file.delim";
  public static final String FILE_ESCAPE = "file.escape";
  private static final String EXTENSION = ".csv";

  @Override
  public String getExtension() {
    return EXTENSION;
  }

  @Override
  public RecordWriter getRecordWriter(final HdfsSinkConnectorConfig conf, final String filename) {

    return new io.confluent.connect.storage.format.RecordWriter() {
      final Path newPath = new Path(filename + "_copy");
      final Path path = new Path(filename);
      CsvHdfsWriter writer = null;

      @Override
      public void write(SinkRecord record) {
        try {
          Configuration hadoopConfiguration = conf.getHadoopConfiguration();
          hadoopConfiguration.setBoolean("dfs.support.append", true);
          hadoopConfiguration.set(
              "dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
          hadoopConfiguration.set(
              "dfs.client.block.write.replace-datanode-on-failure.enable", "true");
          String delimiter = Optional.of(conf.getString(FILE_DELIMITER)).orElse(",");
          writer = new CsvHdfsWriter(hadoopConfiguration, delimiter);
          writeTemp(record);
          writer.append(newPath, path, 4096);
        } catch (IOException | JSONException e) {
          throw new ConnectException(e);
        }
      }

      private void writeTemp(SinkRecord record) throws JSONException, IOException {
        Schema schema = record.valueSchema();
        Struct struct = (Struct) record.value();
        writer.write(schema, struct, newPath);
      }

      @Override
      public void close() {}

      @Override
      public void commit() {}
    };
  }
}
