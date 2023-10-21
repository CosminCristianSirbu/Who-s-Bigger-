package rhul.ac.uk.pagerank;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SparkSession;

import java.io.File;

/**
 * Driver for the naive page rank implementation.
 */
public class NaivePageRankDriver {

  /**
   * Main method of the naive page rank.
   * @param args the command line arguments
   * @throws Exception if the required number of arguments is not provided
   */
  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      System.err.println("Usage: JavaPageRank <input file> <output file>");
      System.exit(1);
    }

    SparkSession spark = SparkSession
        .builder()
        .appName("Naive Page Rank")
        .master("local")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrationRequired", "false")
        .config("spark.driver.memory", "6g")
        .getOrCreate();

    spark
        .sparkContext()
        .setCheckpointDir("./checkpoint");
    String inputPath = args[0];
    String outputPath = args[1];

    FileUtils.deleteQuietly(new File(outputPath));

    NaivePageRank naivePageRank = new NaivePageRank(spark, inputPath, outputPath);
    naivePageRank.runNaivePageRank();

    spark.stop();
  }
}
