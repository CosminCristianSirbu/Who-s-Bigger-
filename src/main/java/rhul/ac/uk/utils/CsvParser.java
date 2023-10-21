package rhul.ac.uk.utils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;

public class CsvParser {
  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println("Usage: ParseJson <input file> <output file>");
      System.exit(1);
    }

    StructType schema = new StructType(
        new StructField[]{new StructField("text", DataTypes.StringType, true,
            Metadata.empty())});

    SparkConf conf = new SparkConf()
        .setMaster("local")
        .setAppName("ParseCsv");

    SparkSession spark = SparkSession
        .builder()
        .config(conf)
        .getOrCreate();

    String inputPath = args[0];
    String outputPath = args[1];

    //delete output path
    FileUtils.deleteQuietly(new File(outputPath));

    JavaRDD<Object> rows = spark
        .read()
        .format("csv")
        .schema(schema)
        .load(inputPath)
        .javaRDD()
        .mapToPair(v -> new Tuple2<>(1, v.get(0)))
        .reduceByKey((a, b) -> a + " " + b)
        .values()
        .coalesce(1);

    JavaRDD<String> cleanedLines = rows.flatMap(line -> {
      String[] words = StringUtils.normalizeSpace(line.toString()
                                                      .toLowerCase()
                                                      .replaceAll("[^a-zA-Z]", " "))
                                  .split("\\W+");

      ArrayList<String> newLines = new ArrayList<>();

      StringBuilder newLine = new StringBuilder();
      for (int i = 0; i < words.length; i++) {
        newLine.append(words[i])
               .append(" ");
        if ((i % 60 == 0 && i != 0) || i == words.length - 1) {
          newLines.add(newLine + ",");
          newLine = new StringBuilder();
        }
      }

      return newLines.iterator();
    });

    cleanedLines.saveAsTextFile(outputPath);

  }
}
