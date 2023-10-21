package rhul.ac.uk.unittests.wordcount.v2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import rhul.ac.uk.wordcountv2.WordCountV2Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

/**
 * Test suite for WordCountV2Reducer.
 */
public class WordCountV2ReducerTest {

  /**
   * Tester var that mocks a mapped word.
   */
  private Text key;

  /**
   * Tester var that mocks a wordCountV1Reducer instance.
   */
  private WordCountV2Reducer wordCountV2Reducer;

  /**
   * Tester var that mocks a list of mapped values for a word.
   */
  private Iterable<LongWritable> values;

  /**
   * Tester var that mocks a context instance.
   */
  private Context context;

  /**
   * Set up method used to create a new key, wordCountV1Reducer instance and mock a new context.
   */
  @BeforeEach
  public void setUp() {
    key = new Text("key");
    wordCountV2Reducer = new WordCountV2Reducer();
    context = mock(Context.class);
  }

  /**
   * Test 0.
   * Test the reducer method with not values to check.
   *
   * @throws IOException          when the input is wrong
   * @throws InterruptedException when the context writing is interrupted
   */
  @Test
  public void testNoValues() throws IOException, InterruptedException {
    values = Collections.emptyList();
    wordCountV2Reducer.reduce(key, values, context);
    InOrder inOrder = inOrder(context);
    inOrder
        .verify(context)
        .write(key, new LongWritable(0));
  }

  /**
   * Test 2.
   * Test the reducer method with a single value to check if the same is collected.
   *
   * @throws IOException          when the input is wrong
   * @throws InterruptedException when the context writing is interrupted
   */
  @Test
  public void testSingleValue() throws IOException, InterruptedException {
    values = Collections.singletonList(new LongWritable(5));
    wordCountV2Reducer.reduce(key, values, context);
    InOrder inOrder = inOrder(context);
    inOrder
        .verify(context)
        .write(key, new LongWritable(5));
  }

  /**
   * Test 3.
   * Test the reducer method to check if three values are summed up correctly.
   *
   * @throws IOException          when the input is wrong
   * @throws InterruptedException when the context writing is interrupted
   */
  @Test
  public void testThreeValues() throws IOException, InterruptedException {
    values = Arrays.asList(new LongWritable(5), new LongWritable(10), new LongWritable(20));
    wordCountV2Reducer.reduce(key, values, context);
    InOrder inOrder = inOrder(context);
    inOrder
        .verify(context)
        .write(key, new LongWritable(35));
  }

  /**
   * Test 4.
   * Test the reducer method to check if many values are summed up correctly.
   *
   * @throws IOException          when the input is wrong
   * @throws InterruptedException when the context writing is interrupted
   */
  @Test
  public void testManyValues() throws IOException, InterruptedException {
    values = Arrays.asList(new LongWritable(5), new LongWritable(10), new LongWritable(20),
        new LongWritable(50), new LongWritable(0), new LongWritable(23));
    wordCountV2Reducer.reduce(key, values, context);
    InOrder inOrder = inOrder(context);
    inOrder
        .verify(context)
        .write(key, new LongWritable(108));
  }
}

