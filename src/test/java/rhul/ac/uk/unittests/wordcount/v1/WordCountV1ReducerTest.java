package rhul.ac.uk.unittests.wordcount.v1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.InOrder;
import rhul.ac.uk.wordcountv1.WordCountV1Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

/**
 * Test suite for WordCountV1Reducer.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class WordCountV1ReducerTest {

  /**
   * Tester var that mocks a mapped word.
   */
  private Text key;

  /**
   * Tester var that mocks a wordCountV1Reducer instance.
   */
  private WordCountV1Reducer wordCountV1Reducer;

  /**
   * Tester var that mocks a list of mapped values for a word.
   */
  private Iterable<IntWritable> values;

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
    wordCountV1Reducer = new WordCountV1Reducer();
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
    wordCountV1Reducer.reduce(key, values, context);
    InOrder inOrder = inOrder(context);
    inOrder
        .verify(context)
        .write(key, new IntWritable(0));
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
    values = Collections.singletonList(new IntWritable(5));
    wordCountV1Reducer.reduce(key, values, context);
    InOrder inOrder = inOrder(context);
    inOrder
        .verify(context)
        .write(key, new IntWritable(5));
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
    values = Arrays.asList(new IntWritable(5), new IntWritable(10), new IntWritable(20));
    wordCountV1Reducer.reduce(key, values, context);
    InOrder inOrder = inOrder(context);
    inOrder
        .verify(context)
        .write(key, new IntWritable(35));
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
    values = Arrays.asList(new IntWritable(5), new IntWritable(10), new IntWritable(20),
        new IntWritable(50), new IntWritable(0), new IntWritable(23));
    wordCountV1Reducer.reduce(key, values, context);
    InOrder inOrder = inOrder(context);
    inOrder
        .verify(context)
        .write(key, new IntWritable(108));
  }
}
