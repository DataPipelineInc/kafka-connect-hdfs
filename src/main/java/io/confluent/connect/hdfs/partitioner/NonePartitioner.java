package io.confluent.connect.hdfs.partitioner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.connect.sink.SinkRecord;

public class NonePartitioner implements Partitioner {
  protected Map<String, Object> config;
  private String delim;

  @Override
  public void configure(Map<String, Object> config) {
    this.config = config;
    this.delim = (String) config.get("directory.delim");
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    return "";
  }

  @Override
  public String generatePartitionedPath(String topic, String encodedPartition) {
    return topic + this.delim + encodedPartition;
  }

  @Override
  public List<FieldSchema> partitionFields() {
    return new ArrayList<>();
  }
}
