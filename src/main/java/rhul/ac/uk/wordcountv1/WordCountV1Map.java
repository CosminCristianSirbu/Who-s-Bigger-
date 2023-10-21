package rhul.ac.uk.wordcountv1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Implementation of the Mapper for the Word Count Problem.
 *
 * @author Cosmin Sirbu
 */
public class WordCountV1Map extends Mapper<Object, Text, Text, IntWritable> {

  /**
   * Receives a partition of the input data, parses it and maps each new word to the value one.
   * Later the combiner will combine the found frequencies of the found words.
   *
   * @param key     the initial system generated key K1 of the input
   * @param value   the initial unparsed value V1 of the input
   * @param context the context used to map key-value pairs
   * @throws IOException          when the input is wrong
   * @throws InterruptedException when the context writing is interrupted
   */
  public void map(Object key, Text value, Context context) throws IOException,
      InterruptedException {
    StringTokenizer parser = new StringTokenizer(value.toString());
    WordCountV1Parser wordCountV1Parser = new WordCountV1Parser();
    while (parser.hasMoreTokens()) {
      wordCountV1Parser.parse(parser.nextToken());
      context.write(wordCountV1Parser.getWord(), wordCountV1Parser.getOne());
    }
  }
}
