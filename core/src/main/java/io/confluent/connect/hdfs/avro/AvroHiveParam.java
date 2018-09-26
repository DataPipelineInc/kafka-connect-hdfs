package io.confluent.connect.hdfs.avro;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class AvroHiveParam implements Serializable {

  private static final long serialVersionUID = 6542653079954461766L;

  private String namespace;
  private String name;
  private String type;
  private List<Map<String, String>> fields;

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public List<Map<String, String>> getFields() {
    return fields;
  }

  public void setFields(List<Map<String, String>> fields) {
    this.fields = fields;
  }
}
