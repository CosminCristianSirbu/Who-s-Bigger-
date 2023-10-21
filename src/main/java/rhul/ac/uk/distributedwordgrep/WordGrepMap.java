package rhul.ac.uk.distributedwordgrep;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.archive.io.ArchiveReader;

/**
 * Wrapper of the mapper implementation for the word grep problem.
 *
 * @author Cosmin Sirbu
 */
public class WordGrepMap {

  /**
   * Implementation of the Word Grep Mapper for the WET WARC files from Common Crawl.
   */
  public static class GrepWordMapper extends Mapper<Text, ArchiveReader, Text, Text> {

    /**
     * Overrides the default map method and uses the map file reader to read,
     * parse and map the input.
     *
     * @param key           the generated Hadoop key
     * @param archiveReader the passed archive reader
     * @param context       the passed context
     */
    public void map(Text key, ArchiveReader archiveReader, Context context) {
      WordGrepMapFacade grepMapperFileReader = new WordGrepMapFacade(archiveReader, context);
      grepMapperFileReader.mapRecord();
    }
  }
}
