package rhul.ac.uk.wordcountv2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * Utility parser used to parse new inputted words.
 *
 * @author Cosmin Sirbu
 */
public class WordCountV2Parser {

  /**
   * Regex black list of chars to remove from entries.
   */
  private static final String blackListRegex = "[^\\p{L}]+";

  /**
   * Value of one that is mapped to each word found.
   */
  private final long one;

  /**
   * Mutable String variable used to save a parsed word.
   */
  private String word;

  /**
   * Constructor of the class. Initializes the fields.
   */
  public WordCountV2Parser() {
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
   * Checks if the word is of a valid format. Not interested in single char words or too long
   * words.
   *
   * @return true if the word is of the right length.
   */
  public boolean isValidWord() {
    return word.length() > 1 && word.length() < 50;
  }

  /**
   * Returns a new IntWritable value of the one.
   *
   * @return the IntWritable value of the one
   */
  public LongWritable getOne() {
    return new LongWritable(this.one);
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

