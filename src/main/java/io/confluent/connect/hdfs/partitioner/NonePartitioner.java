package io.confluent.connect.hdfs.partitioner;

import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.connect.sink.SinkRecord;

public class NonePartitioner implements Partitioner {
  @Override
  public void configure(Map<String, Object> config) {}

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    return null;
  }

  @Override
  public String generatePartitionedPath(String topic, String encodedPartition) {
    return null;
  }

  @Override
  public List<FieldSchema> partitionFields() {
    return null;
  }
}
