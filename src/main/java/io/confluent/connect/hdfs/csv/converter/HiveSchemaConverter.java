package io.confluent.connect.hdfs.csv.converter;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.kafka.connect.data.Schema;

public class HiveSchemaConverter {

  private static final Map<TypeInfo, Schema> TYPE_INFO_TO_TYPE = new HashMap();

  static {
    TYPE_INFO_TO_TYPE.put(TypeInfoFactory.booleanTypeInfo, Schema.BOOLEAN_SCHEMA);
    TYPE_INFO_TO_TYPE.put(TypeInfoFactory.byteTypeInfo, Schema.INT8_SCHEMA);
    TYPE_INFO_TO_TYPE.put(TypeInfoFactory.shortTypeInfo, Schema.INT16_SCHEMA);
    TYPE_INFO_TO_TYPE.put(TypeInfoFactory.intTypeInfo, Schema.INT32_SCHEMA);
    TYPE_INFO_TO_TYPE.put(TypeInfoFactory.longTypeInfo, Schema.INT64_SCHEMA);
    TYPE_INFO_TO_TYPE.put(TypeInfoFactory.floatTypeInfo, Schema.FLOAT32_SCHEMA);
    TYPE_INFO_TO_TYPE.put(TypeInfoFactory.doubleTypeInfo, Schema.FLOAT64_SCHEMA);
    TYPE_INFO_TO_TYPE.put(TypeInfoFactory.binaryTypeInfo, Schema.BYTES_SCHEMA);
    TYPE_INFO_TO_TYPE.put(TypeInfoFactory.stringTypeInfo, Schema.STRING_SCHEMA);
  }

  public static Schema getSchema(TypeInfo typeInfo) {
    return TYPE_INFO_TO_TYPE.get(typeInfo);
  }

  public static Schema getSchema(String typeInfoName) {
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeInfoName);
    return getSchema(typeInfo);
  }
}
