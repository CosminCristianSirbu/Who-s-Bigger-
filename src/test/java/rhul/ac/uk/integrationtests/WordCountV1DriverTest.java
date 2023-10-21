package rhul.ac.uk.integrationtests;

import org.apache.commons.lang.time.StopWatch;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rhul.ac.uk.wordcountv1.WordCountV1Driver;

import static org.awaitility.Awaitility.await;

/**
 * Test class suite for the Word Count v1 MapReduceDriver.
 */
public class WordCountV1DriverTest {

  /**
   * The base url for the RHUL Big Data Cluster.
   */
  private final String hdfsUrl =
      "hdfs://bigdata.cim.rhul.ac" + ".uk:8020/user/zhac254/final-year-project";

  /**
   * Tester var to mock a WordCount MapReduceDriver.
   */
  private WordCountV1Driver driver;

  /**
   * Tester var used to measure execution times.
   */
  private StopWatch watch;

  /**
   * Set up method used to create a new key, wordReducer instance and mock a new context.
   */
  @BeforeEach
  public void setUp() {
    driver = new WordCountV1Driver();
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
    WordCountV1Driver.main(args);
    await().until(() -> driver.getExitCode() == 0);
    watch.stop();
  }

  /**
   * Test 1.
   * Tests if the Word Count correctly executes for the 1000 words file.
   * It also checks if the execution time < 4 sec.
   *
   * @throws Exception when any run time exception is encountered
   */
  @Test
  public void testExecutionLoremIpsum1000() throws Exception {
    executeWordCount(new String[]{
        hdfsUrl + "/input/text/loremipsum-1000words.txt", hdfsUrl + "/output/wordcount/v1"});
    Assertions.assertEquals(driver.getExitCode(), 0);
    System.out.println(
        "Word Count Lorem Ipsum Test 1000 words time (seconds)" + watch.getTime() / 1000);
  }

  /**
   * Test 2.
   * Tests if the Word Count correctly executes for the 10000 words file.
   * It also checks if the execution time < 4 sec.
   *
   * @throws Exception when any run time exception is encountered
   */
  @Test
  public void testExecutionLoremIpsum10000() throws Exception {
    executeWordCount(new String[]{
        hdfsUrl + "/input/text/loremipsum-10000words.txt", hdfsUrl + "/output/wordcount/v1"});
    Assertions.assertEquals(driver.getExitCode(), 0);
    System.out.println(
        "Word Count Lorem Ipsum Test 10000 words time (seconds) " + watch.getTime() / 1000);
  }

  /**
   * Test 3.
   * Tests if the Word Count correctly executes for the 100000 words file.
   * It also checks if the execution time < 4 sec.
   *
   * @throws Exception when any run time exception is encountered
   */
  @Test
  public void testExecutionLoremIpsum100000() throws Exception {
    executeWordCount(new String[]{
        hdfsUrl + "/input/text/loremipsum-100000words.txt", hdfsUrl + "/output/wordcount/v1"});
    Assertions.assertEquals(driver.getExitCode(), 0);
    System.out.println(
        "Word Count Lorem Ipsum Test 100000 words time (seconds) " + watch.getTime() / 1000);
  }
}