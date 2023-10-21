package rhul.ac.uk.pagerank;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.lib.PageRank;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

/**
 * Complete implementation of the page rank program on wiki pages.
 */
public class WikiPageRank {

  /**
   * Tag for the page rank graph.
   */
  private static final ClassTag<String> tagString = ClassTag$.MODULE$.apply(String.class);

  /**
   * The probability that the page rank randomly visits another node during execution.
   */
  private final static double resetProbability = 0.15;

  /**
   * The maximum amount of change between iterations of the page rank.
   */
  private final static double convergence = 0.001;

  /**
   * The output path to save the page rank result.
   */
  private final String outputPath;

  /**
   * The wiki pages vertices processed by the wiki page parser.
   */
  private final JavaRDD<Tuple2<Object, String>> vertices;

  /**
   * The wiki biography pages created by the wiki page parser.
   */
  private final JavaPairRDD<String, Boolean> biographyPages;

  /**
   * The vertices of the page rank graph.
   */
  private JavaPairRDD<Object, Object> pageRankVertices;

  /**
   * An RDD to store the wiki page and its page rank.
   */
  private JavaPairRDD<String, String> pageRanks;

  /**
   * A list of sorted biographic page ranks
   */
  private JavaRDD<String> sortedPersonPageRanks;

  /**
   * The GraphX used to run the page rank.
   */
  private final Graph<String, String> pageRankGraph;

  /**
   * The constructor of the class.
   * Initializes the values.
   *
   * @param outputPath the path where the biographical page ranks is saved
   * @param edges the edges of the page rank graph
   * @param vertices the vertices of the page rank graph
   * @param biographyPages the wiki biographical pages
   */
  public WikiPageRank(String outputPath, JavaRDD<Edge<String>> edges, JavaRDD<Tuple2<Object, String>> vertices,
                      JavaPairRDD<String, Boolean> biographyPages) {
    this.outputPath = outputPath;
    this.vertices = vertices;
    this.biographyPages = biographyPages;
    this.pageRankVertices = null;
    this.pageRanks = null;
    this.sortedPersonPageRanks=null;
    this.pageRankGraph = Graph.apply(vertices.rdd(), edges.rdd(), "", StorageLevel.MEMORY_ONLY_SER(),
                                     StorageLevel.MEMORY_ONLY_SER(), tagString, tagString);
  }

  /**
   * Runs the page rank algorithm.
   */
  public void runPageRank() {
    this.computePageRank();
    this.parsePageRankValues();
    this.sortPageRankValues();
    this.savePageRankResult();
  }

  /**
   * Saves the sorted biographical pages with their page ranks.
   */
  private void savePageRankResult() {
    sortedPersonPageRanks
        .coalesce(1)
        .saveAsTextFile(outputPath);
  }

  /**
   * Sorts the page rank values descending.
   */
  private void sortPageRankValues() {
    sortedPersonPageRanks = pageRanks
        .join(biographyPages)
        .mapToPair(v -> new Tuple2<>(v
                                         ._2()
                                         ._1(), v._1()))
        .sortByKey(false)
        .map(v -> v._2().replaceAll(",", " ") + "," + v._1());
  }

  /**
   * Parses the page rank values from the graph.
   */
  private void parsePageRankValues() {
    pageRanks = pageRankVertices
        .join(vertices.mapToPair(v -> v))
        .mapToPair(Tuple2::_2)
        .mapToPair(v -> new Tuple2<>(v._2(), v
            ._1()
            .toString()))
        .persist(StorageLevel.MEMORY_ONLY_SER());
  }

  /**
   * Runs the page rank algorithm using the GrapX pageRank algorithm.
   */
  private void computePageRank() {
    pageRankVertices = PageRank
        .runUntilConvergence(pageRankGraph, convergence, resetProbability, null, null)
        .vertices()
        .toJavaRDD()
        .mapToPair(v -> v)
        .persist(StorageLevel.MEMORY_ONLY_SER());
  }
}
