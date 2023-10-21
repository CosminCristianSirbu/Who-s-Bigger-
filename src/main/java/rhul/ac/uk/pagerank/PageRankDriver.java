package rhul.ac.uk.pagerank;

import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.Serializable;

/**
 * The driver for the wiki page rank problem.
 */
public class PageRankDriver implements Serializable {

  /**
   * The main method of the wiki page rank program.
   *
   * @param args the command line arguments
   * @throws IOException if the number of command line arguments is wrong
   */
  public static void main(String[] args) throws IOException {
    if (args.length != 3) {
      System.err.println(
          "There must be 3 command line arguments: deploy mode (cluster/local), input file and output file");
      System.exit(1);
    }

    String deployMode = args[0];
    String inputPath = args[1];
    String outputPath = args[2];
    SparkSession spark = null;

    if (!deployMode.equals("cluster") && !deployMode.equals("local")) {
      System.err.println("Deploy mode (args[0]) can only be cluster/local");
    }

    if (deployMode.equals("cluster")) {
      spark = SparkSession
          .builder()
          .master("yarn")
          .appName("Page Rank")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.kryo.registrationRequired", "false")
          .config("spark.executor.cores", "3")
          .config("spark.executor.memory", "20g")
          .config("spark.executor.instances", "10")
          .config("spark.sql.shuffle.partitions", "3")
          .getOrCreate();
      spark
          .sparkContext()
          .setLogLevel("ERROR");
    } else if (deployMode.equals("local")) {
      spark = SparkSession
          .builder()
          .master("local")
          .appName("Page Rank")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.kryo.registrationRequired", "false")
          .config("spark.default.parallelism", "1")
          .config("spark.driver.memory", "6g")
          .config("spark.sql.shuffle.partitions", "1")
          .getOrCreate();
      spark
          .sparkContext()
          .setLogLevel("INFO");
    }

    WikiPageParser wikiPageParser = new WikiPageParser(inputPath, spark);
    wikiPageParser.createWikiPagesGraph();
    WikiPageRank wikiPageRank = new WikiPageRank(outputPath, wikiPageParser.getEdges(), wikiPageParser.getVertices(),
                                                 wikiPageParser.getBiographyPages());
    wikiPageRank.runPageRank();
  }
}
