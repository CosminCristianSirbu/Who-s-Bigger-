package rhul.ac.uk.unittests.distributedwordgrep;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import rhul.ac.uk.distributedwordgrep.WordGrepReducer;

import java.io.IOException;
import java.util.Collections;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

/**
 * Test suite for the Word Grep Reducer.
 */
public class WordGrepReducerTest {

  /**
   * Tester var to mock a reducer.
   */
  private WordGrepReducer reducer;

  /**
   * Tester var to mock a website.
   */
  private Text website;

  /**
   * Tester var to mock a list of input values.
   */
  private Iterable<Text> values;

  /**
   * Tester var that mocks a context instance.
   */
  private Context context;

  /**
   * Tester var to mock to reduce output.
   */
  private Text output;

  /**
   * Set up done before each test.
   */
  @BeforeEach
  public void setUp() {
    reducer = new WordGrepReducer();
    website = new Text();
    context = mock(Context.class);
    output = new Text();
  }

  /**
   * Test 1.
   * Test the reducer method with a single value to check if the same is collected.
   *
   * @throws IOException          when the input is wrong
   * @throws InterruptedException when the context writing is interrupted
   */
  @Test
  public void testSingleValue() throws IOException, InterruptedException {
    website.set("https://test1.com");
    reducer.setSearchRegex("football");
    values = Collections.singletonList(new Text("Football!"));
    output.set(
        "\n" + "football found at positions: \n" + "Line 1 - positions: 0\n" + "\n" + "1" + "."
            + " " + "Football!\n");
    reducer.reduce(website, values, context);
    InOrder inOrder = inOrder(context);
    inOrder
        .verify(context)
        .write(website, output);
  }

  /**
   * Test 2.
   * Test if the reducer correctly returns for one word occurrence.
   *
   * @throws IOException          when the input is wrong
   * @throws InterruptedException when the context writing is interrupted
   */
  @Test
  public void testOneOccurrence() throws IOException, InterruptedException {
    website.set("https://test2.com");
    reducer.setSearchRegex("lie");
    values = Collections.singletonList(new Text(
        "100% Injury Rate: VICTORY IS OURS!Thursday, " + "May" + " " + "3, "
            + "2007VICTORY IS OURS!We're not going to lie, we we"));
    output.set(
        "\n" + "lie found at positions: \n" + "Line 1 - positions: 90\n" + "\n" + "1. " + "100%"
            + " Injury Rate: VICTORY IS OURS!Thursday, May 3, 2007VICTORY IS OURS!We're "
            + "not going " + "to lie, we we\n");
    reducer.reduce(website, values, context);
    InOrder inOrder = inOrder(context);
    inOrder
        .verify(context)
        .write(website, output);
  }

  /**
   * Test 3.
   * Test if the reducer correctly returns for multiple word occurrences.
   *
   * @throws IOException          when the input is wrong
   * @throws InterruptedException when the context writing is interrupted
   */
  @Test
  public void testMultiOccurrences() throws IOException, InterruptedException {
    website.set("https://test3.com");
    reducer.setSearchRegex("victory");
    values = Collections.singletonList(new Text(
        "100% Injury Rate: VICTORY IS OURS!Thursday," + "May 3, 2007VICTORY IS OURS!We're not "
            + "going to lie, we we"));
    output.set("\n" + "victory found at positions: \n" + "Line 1 - positions: 18,54\n" + "\n"
        + "1. 100% Injury Rate: VICTORY IS OURS!Thursday,May 3, 2007VICTORY IS "
        + "OURS!We're not " + "going to lie, we we\n");
    reducer.reduce(website, values, context);
    InOrder inOrder = inOrder(context);
    inOrder
        .verify(context)
        .write(website, output);
  }

  /**
   * Test 4.
   * Test if the reducer correctly returns for multiple occurrences across multiple lines.
   *
   * @throws IOException          when the input is wrong
   * @throws InterruptedException when the context writing is interrupted
   */
  @Test
  public void testMultiLineOccurrences() throws IOException, InterruptedException {
    website.set("https://test4.com");
    reducer.setSearchRegex("not");
    values = Collections.singletonList(new Text(
        "100% Injury Rate: VICTORY IS OURS!Thursday, " + "May" + " " + "3,"
            + "2007VICTORY IS OURS!We're not going to lie, we we" + "\nre " + "crapping " + "our "
            + "pants" + " when BDizzle pulled that hammy " + "5 minutes into " + "the game. Not "
            + "to mention " + "his kne"));
    output.set("\n" + "not found at positions: \n" + "Line 1 - positions: 76\n" + "Line 2 - "
        + "positions: 76\n" + "\n" + "1. 100% Injury Rate: VICTORY IS OURS!Thursday, May 3,"
        + "2007VICTORY IS OURS!We're not going to lie, we we\n" + "re\n" + "2.  crapping our "
        + "pants when BDizzle pulled that hammy 5 minutes into the game. Not to " + "mention his "
        + "kne\n");
    reducer.reduce(website, values, context);
    InOrder inOrder = inOrder(context);
    inOrder
        .verify(context)
        .write(website, output);
  }

  /**
   * Test 5.
   * Test if the expression is not found then nothing is written to the context.
   *
   * @throws IOException          when the input is wrong
   * @throws InterruptedException when the context writing is interrupted
   */
  @Test
  public void testNoOccurrences() throws IOException, InterruptedException {
    website.set("https://test4.com");
    reducer.setSearchRegex("tennis");
    values = Collections.singletonList(new Text(
        "100% Injury Rate: VICTORY IS OURS!Thursday, " + "May" + " " + "3,"
            + "2007VICTORY IS OURS!We're not going to lie, we we" + "\nre " + "crapping " + "our "
            + "pants" + " when BDizzle pulled that hammy " + "5 minutes into " + "the game. Not "
            + "to mention " + "his kne"));
    output.set("");
    reducer.reduce(website, values, context);
    InOrder inOrder = inOrder(context);
    inOrder
        .verify(context)
        .write(website, output);
  }
}
