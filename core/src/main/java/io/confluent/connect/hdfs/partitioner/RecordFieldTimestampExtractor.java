package io.confluent.connect.hdfs.partitioner;

import io.confluent.connect.storage.errors.PartitionException;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.partitioner.TimestampExtractor;
import java.util.Date;
import java.util.Map;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordFieldTimestampExtractor implements TimestampExtractor {
  private static final Logger log = LoggerFactory.getLogger(RecordFieldTimestampExtractor.class);

  private String fieldName;
  private DateTimeFormatter dateTime;

  @Override
  public void configure(Map<String, Object> config) {
    fieldName = (String) config.get(PartitionerConfig.TIMESTAMP_FIELD_NAME_CONFIG);
    dateTime = ISODateTimeFormat.dateTime();
  }

  @Override
  public Long extract(ConnectRecord<?> record) {
    Object value = record.value();
    if (value instanceof Struct) {
      Struct struct = (Struct) value;
      Object timestampValue = struct.get(fieldName);
      Schema valueSchema = record.valueSchema();
      Schema fieldSchema = valueSchema.field(fieldName).schema();

      if (Timestamp.LOGICAL_NAME.equals(fieldSchema.name())) {
        return ((Date) timestampValue).getTime();
      }

      switch (fieldSchema.type()) {
        case INT32:
        case INT64:
          return ((Number) timestampValue).longValue();
        case STRING:
          return dateTime.parseMillis((String) timestampValue);
        default:
          log.error(
              "Unsupported type '{}' for user-defined timestamp field.",
              fieldSchema.type().getName());
          throw new PartitionException(
              "Error extracting timestamp from record field: " + fieldName);
      }
    } else if (value instanceof Map) {
      Map<?, ?> map = (Map<?, ?>) value;
      Object timestampValue = map.get(fieldName);
      if (timestampValue instanceof Number) {
        return ((Number) timestampValue).longValue();
      } else if (timestampValue instanceof String) {
        return dateTime.parseMillis((String) timestampValue);
      } else if (timestampValue instanceof Date) {
        return ((Date) timestampValue).getTime();
      } else {
        log.error(
            "Unsupported type '{}' for user-defined timestamp field.", timestampValue.getClass());
        throw new PartitionException("Error extracting timestamp from record field: " + fieldName);
      }
    } else {
      log.error("Value is not of Struct or Map type.");
      throw new PartitionException("Error encoding partition.");
    }
  }
}
