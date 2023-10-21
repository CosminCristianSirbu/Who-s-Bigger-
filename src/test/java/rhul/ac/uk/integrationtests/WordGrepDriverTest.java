package rhul.ac.uk.integrationtests;

import org.apache.commons.lang.time.StopWatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rhul.ac.uk.distributedwordgrep.WordGrepDriver;

import static org.awaitility.Awaitility.await;

/**
 * Testing suite for the checking the performance of the Word Grep MapReduceDriver.
 *
 * @author Cosmin Sirbu
 */
public class WordGrepDriverTest {

  /**
   * The base url for the RHUL Big Data Cluster.
   */
  private String hdfsUrl = "hdfs://bigdata.cim.rhul.ac.uk:8020/user/zhac254/final-year-project";

  /**
   * Tester var to be used to mock a Word Grep MapReduceDriver.
   */
  private WordGrepDriver driver;
  /**
   * Tester var to be used to check the performance of the MapReduceDriver executions.
   */
  private StopWatch watch;

  /**
   * Set up method used to create a new MapReduceDriver and watch.
   */
  @BeforeEach
  public void setUp() {
    driver = new WordGrepDriver();
    watch = new StopWatch();
  }

  /**
   * Helper method to execute the  Word Grep MapReduceDriver with the provided args.
   *
   * @param args the input/output args
   * @throws Exception when a run time execution is encountered
   */
  private void executeWordGrep(String[] args) throws Exception {
    watch.start();
    WordGrepDriver.main(args);
    await().until(() -> driver.getExitCode() == 0);
    watch.stop();
  }

  /**
   * Test 1.
   * Tests if the Word Grep correctly executes for the 60k website Common Crawl repo.
   * It also checks if the execution time < 30 seconds.
   *
   * @throws Exception when a run time execution is encountered
   */
  @Test
  public void testCommonCrawlRepo() throws Exception {
    executeWordGrep(new String[]{
        hdfsUrl + "/input/archived/CC-MAIN-20220624213908-20220625003908-00000.warc.wet.gz",
        hdfsUrl + "/output/wordgrep"});
    System.out.println("Common Crawl Word Count Test (seconds) " + watch.getTime() / 1000);
  }
}