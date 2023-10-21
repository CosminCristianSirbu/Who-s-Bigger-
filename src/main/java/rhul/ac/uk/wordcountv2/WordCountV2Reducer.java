package rhul.ac.uk.wordcountv2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Implementation of the Reducer for the Word Count Problem.
 *
 * @author Cosmin Sirbu
 */
public class WordCountV2Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {

  /**
   * Variable that holds the final result of the word frequency analysis addition.
   */
  private final LongWritable result = new LongWritable();

  /**
   * Receives a partition of the mapped input data holding a word and a list of mapped frequencies,
   * adds them up and writes the reduced result to the context.
   *
   * @param key     the mapped word
   * @param values  the list of found word frequency values
   * @param context the context used to reduce the mapped values
   * @throws IOException          when the input is wrong
   * @throws InterruptedException when the context writing is interrupted
   */
  public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,
      InterruptedException {
    long sum = 0;
    for (LongWritable v : values) {
      sum += v.get();
    }
    result.set(sum);
    context.write(key, result);
  }
}

