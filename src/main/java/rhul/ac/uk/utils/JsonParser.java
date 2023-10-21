package rhul.ac.uk.utils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.File;

public class JsonParser {

  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println("Usage: ParseJson <input file> <output file>");
      System.exit(1);
    }

    StructType schema = new StructType(
        new StructField[]{new StructField("article_text", DataTypes.StringType, true,
            Metadata.empty())});

    SparkConf conf = new SparkConf()
        .setMaster("local")
        .setAppName("ParseJson");

    SparkSession spark = SparkSession
        .builder()
        .config(conf)
        .getOrCreate();

    String inputPath = args[0];
    String outputPath = args[1];

    //delete output path
    FileUtils.deleteQuietly(new File(outputPath));

    Dataset<Row> lines = spark
        .read()
        .format("json")
        .schema(schema)
        .load(inputPath)
        .coalesce(1);

    Dataset<String> cleanedLines = lines.map(
        (MapFunction<Row, String>) line -> StringUtils.normalizeSpace(line
            .get(0)
            .toString()
            .replaceAll("[^a-zA-Z]", " ")
            .toLowerCase()), Encoders.STRING());

    cleanedLines
        .write()
        .csv(outputPath);
  }
}
