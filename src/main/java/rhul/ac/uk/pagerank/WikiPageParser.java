package rhul.ac.uk.pagerank;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.graphx.Edge;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses wiki pages and creates a graph to be used by the page rank program.
 */
public class WikiPageParser implements Serializable {
  /**
   * The format of wikipedia links.
   */
  private final Pattern wikiLinkFormat;

  /**
   * The schema to parse wikipedia pages xml files.
   */
  private final StructType wikiSchema;

  /**
   * The input path of the program.
   */
  private final String inputPath;

  /**
   * The current spark session.
   */
  private final SparkSession spark;

  /**
   * The list of all the wikipedia person info boxes.
   */
  private final ArrayList<String> wikiPersonInfoBoxes;
  /**
   * The biography pages of the wikipedia pages.
   */
  private JavaPairRDD<String, Boolean> biographyPages;
  /**
   * The edges of the Wikipedia page graph.
   */
  private JavaRDD<Edge<String>> edges;
  /**
   * The vertices of the Wikipedia page graph.
   */
  private JavaRDD<Tuple2<Object, String>> vertices;
  /**
   * An RDD with each vertex mapped to its id.
   */
  private JavaPairRDD<String, Long> vertexIds;

  /**
   * An RDD with the wiki pages connections tuples.
   */
  private JavaPairRDD<String, String> wikiPages;

  /**
   * And RDD of wiki pages and their connections.
   */
  private JavaRDD<Tuple3<String, HashSet<String>, Boolean>> parsedWikiPages;

  /**
   * The constructor of the class.
   * Initializes the variables.
   *
   * @param inputPath the input path of the program
   * @param spark     the current spark session
   */
  public WikiPageParser(String inputPath, SparkSession spark) {
    this.inputPath = inputPath;
    this.spark = spark;
    this.wikiPersonInfoBoxes = new PersonInfoBoxLoader().loadPersonInfoBoxes();
    this.wikiSchema = new StructType()
        .add(new StructField("title", DataTypes.StringType, false, Metadata.empty()))
        .add(new StructField("revision", DataTypes.createStructType(
            new StructField[]{new StructField("text", DataTypes.StringType, true, Metadata.empty())}), true,
                             Metadata.empty()));
    this.wikiLinkFormat = Pattern.compile("(?<=\\[\\[)[^\\[\\]]*(?=\\]\\])");
    this.edges = null;
    this.vertices = null;
    this.biographyPages = null;
    this.vertexIds = null;
    this.wikiPages = null;
  }

  /**
   * Getter for the Wikipedia graph edges.
   *
   * @return the Wikipedia graph edges
   */
  public JavaRDD<Edge<String>> getEdges() {
    return edges;
  }

  /**
   * Getter for the Wikipedia graph vertices.
   *
   * @return the Wikipedia graph vertices
   */
  public JavaRDD<Tuple2<Object, String>> getVertices() {
    return vertices;
  }

  /**
   * Getter for the wikipedia biography pages.
   *
   * @return the Wikipedia biography pages
   */
  public JavaPairRDD<String, Boolean> getBiographyPages() {
    return biographyPages;
  }

  /**
   * Checks if a Wikipedia page is a biographical page using info boxes.
   *
   * @param wikiPage the wiki page to be checked
   * @return true if the page is biographical, false otherwise
   */
  private boolean checkPageIsBiography(String wikiPage) {
    boolean isBiography = false;
    for (String personInfoBox : wikiPersonInfoBoxes) {
      if (wikiPage.contains("{{" + personInfoBox)) {
        isBiography = true;
        break;
      }
    }
    return isBiography;
  }

  /**
   * Parses a wikipedia page and crates a list of all its external page connections using the wiki internal link format.
   *
   * @param title    the title of the page
   * @param wikiPage the page to be parsed
   * @return the parsed page and its links
   */
  private Tuple3<String, HashSet<String>, Boolean> parseWikiPage(String title, String wikiPage) {
    boolean isBiographicPage = checkPageIsBiography(wikiPage);
    Matcher linkMatcher = wikiLinkFormat.matcher(wikiPage);
    HashSet<String> links = new HashSet<>();

    while (linkMatcher.find()) {
      String link = linkMatcher.group();
      if (link.contains("File:") || link.contains("Category:")
          || link.contains("Help:") || link.contains("commons:")) {
        continue;
      } else if (link.contains("#")) {
        link = link.substring(link.indexOf("|") + 1);
      } else if (link.contains("|")) {
        link = link.substring(0, link.indexOf("|"));
      }
      links.add(StringUtils.normalizeSpace(link));
    }

    return new Tuple3<>(title, links, isBiographicPage);
  }

  /**
   * Creates a Wikipedia page connections graph to be used by the page rank program.
   */
  public void createWikiPagesGraph() {
    parseWikiPages();
    createWikiPageTuples();
    createVertexIds();
    createGraphEdges();
    createGraphVertices();
    createBiographyPages();
  }

  /**
   * Reads a wikipedia xml dump and parses it.
   */
  private void parseWikiPages() {
    parsedWikiPages = spark
        .read()
        .format("xml")
        .option("rowTag", "page")
        .schema(wikiSchema)
        .load(inputPath)
        .javaRDD()
        .map((row) -> parseWikiPage(row
                                        .get(0)
                                        .toString(), row
                                        .get(1)
                                        .toString()))
        .persist(StorageLevel.MEMORY_ONLY_SER());
  }

  /**
   * Creates an RDD with the Wikipedia page tuples.
   */
  private void createWikiPageTuples() {
    wikiPages = parsedWikiPages
        .flatMapToPair(v -> v
            ._2()
            .stream()
            .map(link -> new Tuple2<>(v._1(), link))
            .iterator())
        .persist(StorageLevel.MEMORY_ONLY_SER());
  }

  /**
   * Creates a list with unique ids for each wiki page.
   */
  private void createVertexIds() {
    vertexIds = wikiPages
        .map(Tuple2::_2)
        .distinct()
        .zipWithUniqueId()
        .persist(StorageLevel.MEMORY_ONLY_SER());
  }

  /**
   * Creates an RDD with the biographical wikipedia pages.
   */
  private void createBiographyPages() {
    biographyPages = parsedWikiPages
        .mapToPair(v -> new Tuple2<>(v._1(), v._3()))
        .filter(Tuple2::_2);
  }

  /**
   * Creates an RDD with the Wikipedia pages connections graph vertices.
   */
  private void createGraphVertices() {
    vertices = vertexIds.map(v -> new Tuple2<>(v._2(), v._1()));
  }

  /**
   * Creates an RDD with the Wikipedia pages connections graph edges.
   */
  private void createGraphEdges() {
    edges = wikiPages
        .join(vertexIds)
        .map(Tuple2::_2)
        .mapToPair((v) -> v)
        .join(vertexIds)
        .map((edge) -> new Edge<>(edge
                                      ._2()
                                      ._1(), edge
                                      ._2()
                                      ._2(), ""));

  }
}
