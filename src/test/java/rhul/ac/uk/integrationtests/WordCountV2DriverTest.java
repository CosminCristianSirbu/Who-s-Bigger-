package rhul.ac.uk.integrationtests;

import org.apache.commons.lang.time.StopWatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rhul.ac.uk.wordcountv2.WordCountV2Driver;

import static org.awaitility.Awaitility.await;

/**
 * Testing suite for checking the performance of the WET Count MapReduceDriver.
 *
 * @author Cosmin Sirbu
 */
public class WordCountV2DriverTest {

  /**
   * The base url for the RHUL Big Data Cluster.
   */
  private final String hdfsUrl =
      "hdfs://bigdata.cim.rhul.ac" + ".uk:8020/user/zhac254/final-year-project";

  /**
   * Tester var to mock a WordCount MapReduceDriver.
   */
  private WordCountV2Driver driver;

  /**
   * Tester var used to measure execution times.
   */
  private StopWatch watch;

  /**
   * Set up method used to create a new MapReduceDriver and watch.
   */
  @BeforeEach
  public void setUp() {
    driver = new WordCountV2Driver();
    watch = new StopWatch();
  }

  /**
   * Helper method to execute the word count class with the provided args.
   *
   * @param args the input/output args
   * @throws Exception when any run time exception is encountered
   */
  public void executeWordCount(String[] args) throws Exception {
    watch.start();
    WordCountV2Driver.main(args);
    await().until(() -> driver.getExitCode() == 0);
    watch.stop();
  }

  /**
   * Test 1.
   * Tests if the Word Count correctly executes for the 60k website Common Crawl repo.
   * It also checks if the execution time < 4 minutes.
   *
   * @throws Exception when any run time exception is encountered
   */
  @Test
  public void testCommonCrawlRepo() throws Exception {
    executeWordCount(new String[]{
        hdfsUrl + "/input/archived/CC-MAIN-20220624213908-20220625003908-00000.warc.wet.gz",
        hdfsUrl + "/output/wordcount/v2"});
    System.out.println("Common Crawl Word Count Test (seconds) " + watch.getTime() / 1000);
  }
}