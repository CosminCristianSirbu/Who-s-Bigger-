package rhul.ac.uk.wordcountv3;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SparkSession;

import java.io.File;

/**
 * Driver for the Spark Word Count
 */
public class WordCountV3Driver {

  /**
   * Main class for the spark word count.
   * @param args the command line arguments
   * @throws Exception if the number of command line arguments is wrong
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: JavaWordCount <input_file> <output_file>");
      System.exit(1);
    }

    SparkSession spark = SparkSession
        .builder()
        .appName("JavaWordCount")
        .master("local")
        .getOrCreate();

    String inputPath = args[0];
    String outputPath = args[1];

    WordCountV3 wordCountV3 = new WordCountV3(spark, inputPath, outputPath);
    wordCountV3.runWordCount();

    spark.stop();
  }
}
