package io.confluent.connect.hdfs.hive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;

public class HiveConverter {

  private static final Map<Type, TypeInfo> TYPE_TO_TYPEINFO = new HashMap<>();

  public HiveConverter() {}

  public static List<FieldSchema> convertSchema(Schema schema) {
    List<FieldSchema> columns = new ArrayList<>();
    if (Type.STRUCT.equals(schema.type())) {
      Iterator var2 = schema.fields().iterator();

      while (var2.hasNext()) {
        Field field = (Field) var2.next();
        columns.add(
            new FieldSchema(
                field.name(), convert(field.schema()).getTypeName(), field.schema().doc()));
      }
    }

    return columns;
  }

  public static TypeInfo convert(Schema schema) {
    switch (schema.type()) {
      case BYTES:
        if (Decimal.LOGICAL_NAME.equals(schema.name())) {
          return TypeInfoFactory.decimalTypeInfo;
        } else {
          return convertPrimitive(schema);
        }
      case STRUCT:
        return convertStruct(schema);
      case ARRAY:
        return convertArray(schema);
      case MAP:
        return convertMap(schema);
      default:
        return convertPrimitive(schema);
    }
  }

  public static TypeInfo convertStruct(Schema schema) {
    List<Field> fields = schema.fields();
    List<String> names = new ArrayList<>(fields.size());
    List<TypeInfo> types = new ArrayList<>(fields.size());
    Iterator var4 = fields.iterator();

    while (var4.hasNext()) {
      Field field = (Field) var4.next();
      names.add(field.name());
      types.add(convert(field.schema()));
    }

    return TypeInfoFactory.getStructTypeInfo(names, types);
  }

  public static TypeInfo convertArray(Schema schema) {
    return TypeInfoFactory.getListTypeInfo(convert(schema.valueSchema()));
  }

  public static TypeInfo convertMap(Schema schema) {
    return TypeInfoFactory.getMapTypeInfo(
        convert(schema.keySchema()), convert(schema.valueSchema()));
  }

  public static TypeInfo convertPrimitive(Schema schema) {
    return TYPE_TO_TYPEINFO.get(schema.type());
  }

  static {
    TYPE_TO_TYPEINFO.put(Type.BOOLEAN, TypeInfoFactory.booleanTypeInfo);
    TYPE_TO_TYPEINFO.put(Type.INT8, TypeInfoFactory.byteTypeInfo);
    TYPE_TO_TYPEINFO.put(Type.INT16, TypeInfoFactory.shortTypeInfo);
    TYPE_TO_TYPEINFO.put(Type.INT32, TypeInfoFactory.intTypeInfo);
    TYPE_TO_TYPEINFO.put(Type.INT64, TypeInfoFactory.longTypeInfo);
    TYPE_TO_TYPEINFO.put(Type.FLOAT32, TypeInfoFactory.floatTypeInfo);
    TYPE_TO_TYPEINFO.put(Type.FLOAT64, TypeInfoFactory.doubleTypeInfo);
    TYPE_TO_TYPEINFO.put(Type.BYTES, TypeInfoFactory.binaryTypeInfo);
    TYPE_TO_TYPEINFO.put(Type.STRING, TypeInfoFactory.stringTypeInfo);
  }
}
