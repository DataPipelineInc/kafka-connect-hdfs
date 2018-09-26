package io.confluent.connect.hdfs.avro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

public class AvroConveter {

  public static String convertString(String table, Schema avroSchema) {
    List<Field> fields = avroSchema.getFields();
    AvroHiveParam avroHiveParam = new AvroHiveParam();
    avroHiveParam.setName(table);
    avroHiveParam.setNamespace(avroSchema.getFullName());
    List<Map<String, String>> avroHiveParamFields = new ArrayList<>();
    fields.forEach(
        field -> {
          Map<String, String> fieldMap = new HashMap<>();
          Schema schema = field.schema();
          String typeName = getTypeName(schema);
          String name = field.name();
          fieldMap.put("name", name);
          fieldMap.put("type", typeName);
          avroHiveParamFields.add(fieldMap);
        });
    avroHiveParam.setType("record");
    avroHiveParam.setFields(avroHiveParamFields);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.valueToTree(avroHiveParam);
    return jsonNode.toString();
  }

  private static String getTypeName(Schema schema) {
    String typeName;
    Type schemaType = schema.getType();
    switch (schemaType) {
      case UNION:
        typeName = getTypeName(schema.getTypes().get(1));
        break;
      default:
        typeName = schemaType.getName();
    }
    return typeName;
  }
}
