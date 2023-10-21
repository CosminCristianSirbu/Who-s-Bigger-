package rhul.ac.uk.wordcountv3;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Spark implementation of the Word Count.
 */
public class WordCountV3 {

  /**
   * Pre-compiled regex to find spaces.
   */
  private static final Pattern SPACE = Pattern.compile(" ");

  /**
   * The current spark context.
   */
  private final SparkSession spark;

  /**
   * The program input path
   */
  private final String inputPath;

  /**
   * The program output path.
   */
  private final String outputPath;

  /**
   * The constructor of the class.
   * Initializes the variables.
   *
   * @param spark the current spark session
   * @param inputPath the input path
   * @param outputPath the output path
   */
  public WordCountV3(SparkSession spark, String inputPath, String outputPath) {
    this.spark = spark;
    this.inputPath = inputPath;
    this.outputPath = outputPath;
  }

  /**
   * Runs the word count program.
   */
  public void runWordCount() {
    //read input file into rdd, here the number of partitions can be set
    JavaRDD<String> lines = spark
        .read()
        .textFile(inputPath)
        .javaRDD();

    //transform each line into a list of words
    JavaRDD<String> words = lines.flatMap(row -> Arrays
        .asList(SPACE.split(row))
        .iterator());

    //map phase: transforms each read word into a tuple (word: 1)
    JavaPairRDD<String, Integer> ones = words.mapToPair(word -> new Tuple2<>(word, 1));

    //reduce phase: transforms each word and its found frequencies to the final frequency
    JavaPairRDD<String, Integer> counts = ones.reduceByKey(Integer::sum);

    counts.saveAsTextFile(outputPath);
  }
}
