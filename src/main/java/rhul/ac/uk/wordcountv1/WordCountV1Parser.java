package rhul.ac.uk.wordcountv1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Utility parser used to parse new inputted words.
 *
 * @author Cosmin Sirbu
 */
public class WordCountV1Parser {

  /**
   * Regex black list to only allow alphanumeric chars across all languages.
   */
  private static final String blackListRegex = "[^\\p{L}]+";

  /**
   * Value of one that is mapped to each word found.
   */
  private final int one;

  /**
   * Mutable String variable used to save a parsed word.
   */
  private String word;

  /**
   * Constructor of the class. Initializes the fields.
   */
  public WordCountV1Parser() {
    one = 1;
    word = "";
  }

  /**
   * Parses a new input string and sets the class var word.
   *
   * @param newWord the new word to be parsed
   */
  public void parse(String newWord) {
    setWord(newWord
        .toLowerCase()
        .replaceAll(blackListRegex, ""));
  }

  /**
   * Returns a new IntWritable value of the one.
   *
   * @return the IntWritable value of the one
   */
  public IntWritable getOne() {
    return new IntWritable(this.one);
  }

  /**
   * Returns a new Text value of the word.
   *
   * @return the Text value of the word
   */
  public Text getWord() {
    return new Text(this.word);
  }

  /**
   * Setter for the word.
   *
   * @param newWord the new word to be set
   */
  private void setWord(String newWord) {
    word = newWord;
  }

}
