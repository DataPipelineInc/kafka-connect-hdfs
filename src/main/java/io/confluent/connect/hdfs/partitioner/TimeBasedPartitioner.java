/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.connect.hdfs.partitioner;

import io.confluent.connect.storage.partitioner.TimestampExtractor;
import java.util.Locale;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.joda.time.DateTimeZone;

public class TimeBasedPartitioner
    extends io.confluent.connect.storage.partitioner.TimeBasedPartitioner<FieldSchema>
    implements Partitioner {

  @Override
  public void configure(Map<String, Object> config) {
    long partitionDurationMsProp =
        Long.parseLong(String.valueOf(config.get("partition.duration.ms")));
    if (partitionDurationMsProp < 0L) {
      throw new ConfigException(
          "partition.duration.ms",
          partitionDurationMsProp,
          "Partition duration needs to be a positive.");
    } else {
      String delim = (String) config.get("directory.delim");
      String pathFormat = (String) config.get("path.format");
      if (!pathFormat.equals("") && !pathFormat.equals(delim)) {
        if (delim.equals(pathFormat.substring(pathFormat.length() - delim.length() - 1))) {
          pathFormat = pathFormat.substring(0, pathFormat.length() - delim.length());
        }

        String localeString = (String) config.get("locale");
        if (localeString.equals("")) {
          throw new ConfigException("locale", localeString, "Locale cannot be empty.");
        } else {
          String timeZoneString = (String) config.get("timezone");
          if (timeZoneString.equals("")) {
            throw new ConfigException("timezone", timeZoneString, "Timezone cannot be empty.");
          } else {
            Locale locale = new Locale(localeString);
            DateTimeZone timeZone = DateTimeZone.forID(timeZoneString);
            this.init(partitionDurationMsProp, pathFormat, locale, timeZone, config);
          }
        }
      } else {
        throw new ConfigException("path.format", pathFormat, "Path format cannot be empty.");
      }
    }
  }

  @Override
  public TimestampExtractor newTimestampExtractor(String extractorClassName) {
    try {
      switch (extractorClassName) {
        case "Wallclock":
        case "Record":
          extractorClassName =
              "io.confluent.connect.storage.partitioner.TimeBasedPartitioner$"
                  + extractorClassName
                  + "TimestampExtractor";
          break;
        case "RecordField":
          extractorClassName =
              io.confluent.connect.hdfs.partitioner.RecordFieldTimestampExtractor.class.getName();
          break;
        default:
          throw new ClassNotFoundException(String.format("Class not find %s", extractorClassName));
      }
      Class<?> klass = Class.forName(extractorClassName);
      if (!TimestampExtractor.class.isAssignableFrom(klass)) {
        throw new ConnectException(
            "Class " + extractorClassName + " does not implement TimestampExtractor");
      }
      return (TimestampExtractor) klass.newInstance();
    } catch (ClassNotFoundException
        | ClassCastException
        | IllegalAccessException
        | InstantiationException e) {
      ConfigException ce =
          new ConfigException("Invalid timestamp extractor: " + extractorClassName);
      ce.initCause(e);
      throw ce;
    }
  }
}
