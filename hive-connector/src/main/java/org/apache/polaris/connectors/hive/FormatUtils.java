package org.apache.polaris.connectors.hive;

import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FormatUtils {
  private static Logger LOGGER = LoggerFactory.getLogger(FormatUtils.class);

  public static final String FORMAT_PROPERTY ="polaris.internal.format";

  // TODO migrate to TableFormat once available
  public static String getTableFormat(Table table) {
    if (table.getParameters().containsKey(FORMAT_PROPERTY)) {
      return table.getParameters().get(FORMAT_PROPERTY);
    }

    String inputFormat = table.getSd().getInputFormat();
    if (inputFormat != null) {
      if (inputFormat.toLowerCase().contains("iceberg")) return "iceberg";
      if (inputFormat.toLowerCase().contains("delta")) return "delta";
      if (inputFormat.toLowerCase().contains("hudi")) return "hudi";
      if (inputFormat.toLowerCase().contains("orc")) return "orc";
      if (inputFormat.toLowerCase().contains("parquet")) return "parquet";
    }
    LOGGER.warn("Unable to infer table format from: " + inputFormat);
    return inputFormat;
  }
}
