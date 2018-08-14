package io.confluent.connect.hdfs.csv;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.storage.format.RecordWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class CsvRecordWriterProvider
    implements io.confluent.connect.storage.format.RecordWriterProvider<HdfsSinkConnectorConfig> {

  public static final String FILE_DELIMITER = "file.delim";
  public static final String FILE_ESCAPE = "file.escape";

  @Override
  public String getExtension() {
    return null;
  }

  @Override
  public RecordWriter getRecordWriter(final HdfsSinkConnectorConfig conf, final String filename) {
    return new io.confluent.connect.storage.format.RecordWriter() {
      final Path path = new Path(filename);

      @Override
      public void write(SinkRecord record) {
        try {
          FileSystem fs = FileSystem.get(conf.getHadoopConfiguration());
          FSDataOutputStream fsDataOutputStream = fs.create(path);
          String value = ((Struct) record.value()).getString("after");
          String delimiter = Optional.of(conf.getString(FILE_DELIMITER)).orElse(",");
          String escape = Optional.of(conf.getString(FILE_ESCAPE)).orElse("\"");
          JSONObject content = new JSONObject(value);
          String csvStr = format(content, delimiter, escape);
          fsDataOutputStream.write(csvStr.getBytes());
        } catch (IOException | JSONException e) {
          throw new ConnectException(e);
        }
      }

      private String format(JSONObject content, String delimiter, String escape) {
        if ("{}".equals(content.toString())) {
          return "";
        }

        List<String> list = new LinkedList<>();
        Iterator iterator = content.keys();
        String key;
        while (iterator.hasNext()) {
          key = iterator.next().toString();
          Object value = content.opt(key);
          if (value.equals(JSONObject.NULL)) {
            list.add("");
          } else {
            // csv sample 111,222,"line string contains , \  "   -->  "111","222","\"line string
            // contains \, \\  """
            list.add((escape + escapeCsvString(content.opt(key).toString()) + escape));
          }
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

      @Override
      public void close() {}

      @Override
      public void commit() {}
    };
  }
}
