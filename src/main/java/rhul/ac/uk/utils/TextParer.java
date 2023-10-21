package rhul.ac.uk.utils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;

public class TextParer {

  private static final int paragraphLength = 60;

  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println("Usage: ParseText <input file> <output file>");
      System.exit(1);
    }

    SparkConf conf = new SparkConf().setMaster("local")
                                    .setAppName("ParseText");

    SparkSession spark = SparkSession.builder()
                                     .config(conf)
                                     .getOrCreate();

    String inputPath = args[0];
    String outputPath = args[1];

    parseWikiText(spark, inputPath, outputPath);
  }

  private static void parseWikiText(SparkSession spark, String inputPath, String outputPath) {
    //delete output path
    FileUtils.deleteQuietly(new File(outputPath));

    JavaRDD<String> lines = spark.sparkContext()
                                 .wholeTextFiles(inputPath, 1)
                                 .toJavaRDD()
                                 .mapToPair(v -> v)
                                 .reduceByKey((a, b) -> a + " " + b)
                                 .values()
                                 .coalesce(1);


    JavaRDD<String> cleanedLines = lines.flatMap(TextParer::breakIntoParagraphs);

    cleanedLines.saveAsTextFile(outputPath);
  }

  private static Iterator<String> breakIntoParagraphs(String line) {
    line = line.toLowerCase()
               .replaceAll("[^a-zA-Z]", " ");

    String[] words = StringUtils.normalizeSpace(line)
                                .split("\\W+");

    ArrayList<String> newLines = new ArrayList<>();

    StringBuilder newLine = new StringBuilder();
    for (int i = 0; i < words.length; i++) {
      newLine.append(words[i])
             .append(" ");
      if ((i % paragraphLength == 0 && i != 0) || i == words.length - 1) {
        newLines.add(newLine + ",");
        newLine = new StringBuilder();
      }
    }

    return newLines.iterator();
  }
}
