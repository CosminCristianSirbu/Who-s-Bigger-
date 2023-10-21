package rhul.ac.uk.pagerank;

import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Naive implementation of the page rank algorithm without the use of any library.
 */
public class NaivePageRank {

  /**
   * The number of iterations to run the page rank computation.
   */
  private final static int iterations = 100;

  /**
   * Pre-computed regex to read the lines of the input file.
   */
  private static final Pattern SPACES = Pattern.compile("\\s+");
  /**
   * The probability that another node is visited during run time (15% is default value).
   */
  private final static double resetProbability = 0.15;
  /**
   * The current spark session passed from the driver.
   */
  private final SparkSession spark;
  /**
   * The input path of the page rank.
   */
  private final String inputPath;
  /**
   * The output path of the page rank.
   */
  private final String outputPath;
  /**
   * The RDD to store the ranks of the graph vertices.
   */
  private JavaPairRDD<String, Double> ranks;
  /**
   * The RDD to store the list of connections of a vertex.
   */
  private JavaPairRDD<String, Iterable<String>> links;

  /**
   * Constructor of the class.
   * Initializes the class variables.
   *
   * @param spark      the current spark session
   * @param inputPath  the input path of the program
   * @param outputPath the directory where the page rank result will be outputted
   */

  public NaivePageRank(SparkSession spark, String inputPath, String outputPath) {
    this.spark = spark;
    this.inputPath = inputPath;
    this.outputPath = outputPath;
    this.ranks = null;
    this.links = null;
  }

  /**
   * Parses the Vertex a - Vertex b tuples from the input file.
   *
   * @param line the line to be parsed
   * @return the parsed line as a tuple
   */
  private static Tuple2<String, String> parseLinksTuple(String line) {
    String[] parts = SPACES.split(line);
    return new Tuple2<>(parts[0], parts[1]);
  }

  /**
   * Loops through the links of a page and explodes all the links list into a
   * list of Page - Rank tuples using as a formula the current page rank / the length of its links list.
   *
   * @param currentPage the current page with its list of page links
   * @return the exploded tuple list
   */
  private static Iterator<Tuple2<String, Double>> pageToOtherPagesContribution(
      Tuple2<Iterable<String>, Double> currentPage) {
    int pageLinksCount = Iterables.size(currentPage._1());
    List<Tuple2<String, Double>> results = new ArrayList<>();
    for (String n : currentPage._1) {
      results.add(new Tuple2<>(n, currentPage._2() / pageLinksCount));
    }
    return results.iterator();
  }

  /**
   * Run the naive page rank algorithm.
   */
  public void runNaivePageRank() {
    this.parseVertexConnections();
    this.computePageRank();
    this.savePageRankResults();
  }

  /**
   * Reads the input file and initializes each vertex with its connections.
   */
  private void parseVertexConnections() {
    JavaRDD<String> lines = this.spark
        .read()
        .textFile(this.inputPath)
        .javaRDD();

    this.links = lines
        .mapToPair(NaivePageRank::parseLinksTuple)
        .distinct()
        .groupByKey()
        .persist(StorageLevel.MEMORY_ONLY_SER());
  }

  /**
   * Collects the results of the page Rank algorithm and dumps it in the output file.
   */
  private void savePageRankResults() {
    JavaRDD<String> output = this.ranks.map(r -> "Vertex " + r._1() + " has rank " + r._2());
    output.saveAsTextFile(outputPath);
  }

  /**
   * Computes the page rank for the parsed vertexes and their connections.
   */
  private void computePageRank() {
    // Loads all URLs with other pages links to from input file and initialize ranks of them to one
    this.ranks = this.links.mapValues(rs -> 1.0);

    for (int i = 0; i < iterations; i++) {
      JavaPairRDD<String, Double> contributions = this.links
          .join(this.ranks)
          .values()
          .flatMapToPair(NaivePageRank::pageToOtherPagesContribution);

      otherPagesToPageContributions(contributions);

      //Because contributions will have an increasingly large lineage, this will cause Stackoverflow
      //errors at large iterations sizes. That's why at every 10 steps checkpoint() is called.
      if (i % 10 == 0) {
        contributions.checkpoint();
      }
    }
  }

  /**
   * Re-Calculates the contributions of other pages to the current page.
   *
   * @param contributions the page to other pages calculated contributions
   */
  private void otherPagesToPageContributions(JavaPairRDD<String, Double> contributions) {
    this.ranks = contributions
        .reduceByKey(Double::sum)
        .mapValues(rank -> resetProbability + rank * (1 - resetProbability));
  }
}
