package rhul.ac.uk.wordcountv2;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import rhul.ac.uk.utils.WarcMap;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Facade for the Word Count V2 problem map. Performs the map logic and hides under mapRecord().
 *
 * @author Cosmin Sirbu
 */
public class WordCountV2MapFacade extends WarcMap {

  /**
   * Utility class used to parse a new word.
   */
  private final WordCountV2Parser wordParserV2;

  /**
   * String reader that splits the input into words.
   */
  private StringTokenizer tokenizer;

  /**
   * The constructor of the Word Count File Reader.
   * Initializes the clas variables.
   *
   * @param newArchiveReader the passed archived reader
   * @param newContext       the passed context
   */
  public WordCountV2MapFacade(ArchiveReader newArchiveReader, Mapper.Context newContext) {
    super(newArchiveReader, newContext);
    wordParserV2 = new WordCountV2Parser();
  }

  /**
   * Overrides the WarcMap method.
   * Transforms the de-archived byte array into a StringTokenizer so words can be parsed.
   *
   * @param record the de-archived record
   * @throws IOException if the file reading or writing is wrong
   */
  @Override
  protected void parseInputData(ArchiveRecord record) throws IOException {
    byte[] rawData = IOUtils.toByteArray(record, record.available());
    String input = new String(rawData);
    tokenizer = new StringTokenizer(input);
  }


  /**
   * Overrides the WarcMap method.
   * Parses the site data word by word, cleans it and writes it to the context.
   *
   * @throws IOException          if the file reading or writing is wrong
   * @throws InterruptedException if file reading in interrupted
   */
  @Override
  protected void mapInputData(ArchiveRecord record) throws IOException, InterruptedException {
    if (!tokenizer.hasMoreTokens()) {
      updateEmptyPagesNo();
    } else {
      while (tokenizer.hasMoreTokens()) {
        wordParserV2.parse(tokenizer.nextToken());
        if (wordParserV2.isValidWord()) {
          context.write(wordParserV2.getWord(), wordParserV2.getOne());
        }
      }
    }
  }
}

