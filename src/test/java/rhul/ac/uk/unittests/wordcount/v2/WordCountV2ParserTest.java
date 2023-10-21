package rhul.ac.uk.unittests.wordcount.v2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import rhul.ac.uk.wordcountv2.WordCountV2Parser;

/**
 * Test class suite for the Word Count v2 word parser.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class WordCountV2ParserTest {

  /**
   * Mock WordCountV1Parser instance used for tests.
   */
  private WordCountV2Parser wordCountV2Parser;

  /**
   * Set up method used in the tests.
   */
  @BeforeAll
  public void setUp() {
    wordCountV2Parser = new WordCountV2Parser();
  }


  /**
   * Test 1.
   * Check if the return word method works fine.
   */
  @Test
  public void testGetWord() {
    wordCountV2Parser.parse("test");
    Assertions.assertEquals(wordCountV2Parser.getWord(), new Text("test"));
  }

  /**
   * Test 2.
   * Check if the return frequency works fine.
   */
  @Test
  public void testGetOne() {
    wordCountV2Parser.parse("test");
    Assertions.assertEquals(wordCountV2Parser.getOne(), new LongWritable(1));
  }

  /**
   * Test 3.
   * Check if the parser removes spaces.
   */
  @Test
  public void testParseSpaces() {
    wordCountV2Parser.parse("test ");
    Assertions.assertEquals(wordCountV2Parser.getWord(), new Text("test"));
  }

  /**
   * Test 4.
   * Check if the parser removed capitalization.
   */
  @Test
  public void testParseCapitalLetters() {
    wordCountV2Parser.parse("TesT");
    Assertions.assertEquals(wordCountV2Parser.getWord(), new Text("test"));
  }

  /**
   * Test 5.
   * Check is the parser removes special characters.
   */
  @Test
  public void testParseSpecialChars() {
    wordCountV2Parser.parse("-=Wo$%rd!:");
    Assertions.assertEquals(wordCountV2Parser.getWord(), new Text("word"));
  }

  /**
   * Test 6.
   * Check if the parser removes all specified black list chars.
   */
  @Test
  public void testAll() {
    wordCountV2Parser.parse("-=Wo$%rd!: ana123");
    Assertions.assertEquals(wordCountV2Parser.getWord(), new Text("wordana"));
  }

  /**
   * Test 7.
   * Check if the parser correctly parses foreign words.
   */
  @Test
  public void testForeignWords() {
    wordCountV2Parser.parse("소프트웨어산업용ｋａｎｅｂｏケイトパウダレスリキッドファンデーション");
    Assertions.assertEquals(wordCountV2Parser.getWord(),
        new Text("소프트웨어산업용ｋａｎｅｂｏケイトパウダレスリキッドファンデーション"));
  }

}