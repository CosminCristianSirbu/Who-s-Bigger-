package rhul.ac.uk.unittests.wordcount.v1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import rhul.ac.uk.wordcountv1.WordCountV1Parser;

/**
 * Test suite for WordCountV1Parser.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class WordCountV1ParserTest {

  /**
   * Mock WordCountV1Parser instance used for tests.
   */
  private WordCountV1Parser wordCountV1Parser;

  /**
   * Set up method used in the tests.
   */
  @BeforeAll
  public void setUp() {
    wordCountV1Parser = new WordCountV1Parser();
  }

  /**
   * Test 1.
   * Check if the return word method works fine.
   */
  @Test
  public void testGetWord() {
    wordCountV1Parser.parse("test");
    Assertions.assertEquals(wordCountV1Parser.getWord(), new Text("test"));
  }

  /**
   * Test 2.
   * Check if the return frequency works fine.
   */
  @Test
  public void testGetOne() {
    wordCountV1Parser.parse("test");
    Assertions.assertEquals(wordCountV1Parser.getOne(), new IntWritable(1));
  }

  /**
   * Test 3.
   * Check if the parser removes spaces.
   */
  @Test
  public void testParseSpaces() {
    wordCountV1Parser.parse("test ");
    Assertions.assertEquals(wordCountV1Parser.getWord(), new Text("test"));
  }

  /**
   * Test 4.
   * Check if the parser removed capitalization.
   */
  @Test
  public void testParseCapitalLetters() {
    wordCountV1Parser.parse("TesT");
    Assertions.assertEquals(wordCountV1Parser.getWord(), new Text("test"));
  }

  /**
   * Test 5.
   * Check is the parser removes special characters.
   */
  @Test
  public void testParseSpecialChars() {
    wordCountV1Parser.parse("-=Wo$%rd!:");
    Assertions.assertEquals(wordCountV1Parser.getWord(), new Text("word"));
  }

  /**
   * Test 6.
   * Check if the parser removes all specified black list chars.
   */
  @Test
  public void testAll() {
    wordCountV1Parser.parse("-=Wo$%rd!: ana123");
    Assertions.assertEquals(wordCountV1Parser.getWord(), new Text("wordana"));
  }

  /**
   * Test 7.
   * Check if the parser correctly parses foreign words.
   */
  @Test
  public void testForeignWords() {
    wordCountV1Parser.parse("소프트웨어산업용ｋａｎｅｂｏケイトパウダレスリキッドファンデーション");
    Assertions.assertEquals(wordCountV1Parser.getWord(),
        new Text("소프트웨어산업용ｋａｎｅｂｏケイトパウダレスリキッドファンデーション"));
  }
}