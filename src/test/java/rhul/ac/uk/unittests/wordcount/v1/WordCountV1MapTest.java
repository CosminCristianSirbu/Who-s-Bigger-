package rhul.ac.uk.unittests.wordcount.v1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.InOrder;
import rhul.ac.uk.wordcountv1.WordCountV1Map;

import java.io.IOException;

import static org.mockito.Mockito.*;

/**
 * Test suite for the Word Count Mapper.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class WordCountV1MapTest {
  /**
   * Tester var used to mock a word mapper.
   */
  private WordCountV1Map mapper;

  /**
   * Tester var used to mock a context.
   */
  private Context context;

  /**
   * Tester var used to mock a IntWritable.
   */
  private IntWritable one;

  /**
   * Tester var used to mock mapper input.s
   */
  private Text input;


  /**
   * Set up method used to create a new mapper, context, word and frequency.
   */
  @BeforeEach
  public void setUp() {
    mapper = new WordCountV1Map();
    context = mock(Context.class);
    input = new Text();
    one = new IntWritable(1);
  }

  /**
   * Test 1.
   * Initial dummy test. Feed to the map a single string and see if it collects it correctly.
   *
   * @throws IOException          when the input is wrong
   * @throws InterruptedException when the context writing is interrupted
   */
  @Test
  public void singleWord() throws IOException, InterruptedException {
    input.set("one");
    mapper.map(new LongWritable(1L), input, context);
    InOrder inOrder = inOrder(context);
    inOrder
        .verify(context)
        .write(new Text("one"), one);
  }

  /**
   * Test 2.
   * Testing if the mapper method can parse multiple words.
   *
   * @throws IOException          when the input is wrong
   * @throws InterruptedException when the context writing is interrupted
   */
  @Test
  public void multipleWords() throws IOException, InterruptedException {
    input.set("one two three");
    mapper.map(new LongWritable(1L), input, context);
    InOrder inOrder = inOrder(context);
    inOrder
        .verify(context)
        .write(new Text("one"), one);
    inOrder
        .verify(context)
        .write(new Text("two"), one);
    inOrder
        .verify(context)
        .write(new Text("three"), one);
  }

  /**
   * Test 3.
   * Test if the mapper collects words with capitalised in the same way.
   *
   * @throws IOException          when the input is wrong
   * @throws InterruptedException when the context writing is interrupted
   */
  @Test
  public void capitalLetterSameWord() throws IOException, InterruptedException {
    input.set("test Test tEsT");
    mapper.map(new LongWritable(1L), input, context);
    InOrder inOrder = inOrder(context);
    inOrder
        .verify(context, times(3))
        .write(new Text("test"), one);
  }

  /**
   * Test 4.
   * Test if the mapper can handle same word with special characters.
   *
   * @throws IOException          when the input is wrong
   * @throws InterruptedException when the context writing is interrupted
   */
  @Test
  public void specialCharacters() throws IOException, InterruptedException {
    input.set("One, one. TwO; two?");
    mapper.map(new LongWritable(1L), input, context);
    InOrder inOrder = inOrder(context);
    inOrder
        .verify(context, times(2))
        .write(new Text("one"), one);
    inOrder
        .verify(context, times(2))
        .write(new Text("two"), one);
  }

  /**
   * Test 6.
   * Test if the mapper can handle foreign words.
   *
   * @throws IOException          when the input is wrong
   * @throws InterruptedException when the context writing is interrupted
   */
  @Test
  public void foreignWords() throws IOException, InterruptedException {
    input.set("ｋａｎｅｂｏケイトパウダレ ﺍﻟﻨﺎﺋﺐ 후기");
    mapper.map(new LongWritable(1L), input, context);
    InOrder inOrder = inOrder(context);
    inOrder
        .verify(context)
        .write(new Text("ｋａｎｅｂｏケイトパウダレ"), one);
    inOrder
        .verify(context)
        .write(new Text("ﺍﻟﻨﺎﺋﺐ"), one);
    inOrder
        .verify(context)
        .write(new Text("후기"), one);
  }

  /**
   * Test 7.
   * Test if passing an empty string to the mapper, nothing will be written to the context.
   *
   * @throws IOException          when the input is wrong
   * @throws InterruptedException when the context writing is interrupted
   */
  @Test
  public void testNoWords() throws IOException, InterruptedException {
    input.set("");
    mapper.map(new LongWritable(1L), input, context);
    verify(context, never()).write(new Text(""), one);
  }
}
