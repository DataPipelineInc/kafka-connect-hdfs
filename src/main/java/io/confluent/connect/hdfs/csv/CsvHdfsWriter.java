package io.confluent.connect.hdfs.csv;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.json.JSONException;
import org.json.JSONObject;

public class CsvHdfsWriter {
  private FileSystem fs;
  private String delimiter;
  private String escape;
  public static final String DEFAULT_ESCAPE = "\"";
  public static final String DEFAULT_DELIMITER = ",";

  public CsvHdfsWriter(HdfsSinkConnectorConfig connectorConfig)
      throws IOException, InterruptedException {
    this(connectorConfig, DEFAULT_DELIMITER, DEFAULT_ESCAPE);
  }

  public CsvHdfsWriter(HdfsSinkConnectorConfig connectorConfig, String delimiter)
      throws IOException, InterruptedException {
    this(connectorConfig, delimiter, DEFAULT_ESCAPE);
  }

  public CsvHdfsWriter(HdfsSinkConnectorConfig connectorConfig, String delimiter, String escape)
      throws IOException, InterruptedException {
    String user = connectorConfig.getString(HdfsSinkConnectorConfig.HADOOP_USER);
    Configuration hadoopConfiguration = connectorConfig.getHadoopConfiguration();
    hadoopConfiguration.setBoolean("dfs.support.append", true);
    hadoopConfiguration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
    hadoopConfiguration.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
    this.fs =
        FileSystem.newInstance(
            FileSystem.getDefaultUri(hadoopConfiguration), hadoopConfiguration, user);
    this.delimiter = delimiter;
    this.escape = escape;
  }

  public void write(Schema schema, Struct struct, Path path) throws IOException, JSONException {
    JSONObject content = parseSinkRecordStruct(struct);
    String csvStr = format(content, schema);
    FSDataOutputStream fsDataOutputStream = fs.create(path);
    fsDataOutputStream.write(csvStr.getBytes());
    fsDataOutputStream.write("\n".getBytes());
    fsDataOutputStream.flush();
    fsDataOutputStream.close();
  }

  public void append(Path source, Path target, int buffSize) throws IOException {
    FSDataOutputStream fsDataOutputStream;
    if (fs.exists(target)) {
      fsDataOutputStream = fs.append(target);
    } else {
      fsDataOutputStream = fs.create(target);
    }
    FSDataInputStream fsDataInputStream = fs.open(source);
    IOUtils.copyBytes(fsDataInputStream, fsDataOutputStream, buffSize, true);
  }

  private String format(JSONObject content, Schema schema) {
    if ("{}".equals(content.toString())) {
      return "";
    }
    List<String> fields = schema.fields().stream().map(Field::name).collect(Collectors.toList());
    List<String> list = new LinkedList<>();
    assert content.length() == fields.size();
    for (String key : fields) {
      Object value = content.opt(key);
      String sinkValue = "";
      if (!value.equals(JSONObject.NULL)) {
        // csv sample 111,222,"line string contains , \  "   -->  "111","222","\"line string
        // contains \, \\  """
        sinkValue = (escape + escapeCsvString(content.opt(key).toString()) + escape);
      }
      list.add(sinkValue);
    }
    return String.join(delimiter, list);
  }

  private String escapeCsvString(String str) {
    if (str == null) {
      return null;
    } else {
      StringWriter out = new StringWriter(str.length() * 2);
      int sz = str.length();

      for (int i = 0; i < sz; ++i) {
        char ch = str.charAt(i);
        if (ch < 32) {
          switch (ch) {
            case '\b':
              out.write(92);
              out.write(98);
              break;
            case '\t':
              out.write(92);
              out.write(116);
              break;
            case '\n':
              out.write(92);
              out.write(110);
              break;
            case '\u000b':
            default:
              if (ch > 15) {
                out.write("\\u00" + hex(ch));
              } else {
                out.write("\\u000" + hex(ch));
              }
              break;
            case '\f':
              out.write(92);
              out.write(102);
              break;
            case '\r':
              out.write(92);
              out.write(114);
          }
        } else if (ch < 128) {
          switch (ch) {
            case '\"':
              out.write(34);
              out.write(34);
              break;
            case '\\':
              out.write(92);
              out.write(92);
              break;
            default:
              out.write(ch);
          }
        } else {
          out.write(ch);
        }
      }
      return out.toString();
    }
  }

  private String hex(char ch) {
    return Integer.toHexString(ch).toUpperCase(Locale.ENGLISH);
  }

  private static JSONObject parseSinkRecordStruct(Struct struct) throws JSONException {
    JSONObject store = new JSONObject();
    if ((struct == null) || (struct.schema() == null)) {
      return store;
    }
    for (Field field : struct.schema().fields()) {
      Object value = struct.get(field);
      Object jsonValue;
      String name = field.schema().name();
      if (value == "\\N" || value == null) {
        jsonValue = JSONObject.NULL;
      } else if (field.schema().type() == Type.BYTES) {
        if (Decimal.LOGICAL_NAME.equals(name)) {
          jsonValue = value;
        } else {
          if (value instanceof ByteBuffer) {
            jsonValue = ((ByteBuffer) value).array();
          } else {
            jsonValue = value;
          }
        }
      } else if (value instanceof Struct) {
        jsonValue = parseSinkRecordStruct((Struct) value);
      } else {
        jsonValue = value;
      }
      store.put(
          field.schema().parameters() == null
              ? field.name()
              : field.schema().parameters().getOrDefault("name", field.name()),
          jsonValue);
    }
    return store;
  }
}
