package rhul.ac.uk.wordcountv2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.archive.io.ArchiveReader;

/**
 * Wrapper over the Word Count v2 map class.
 *
 * @author Cosmin Sirbu
 */
public class WordCountV2Map {
  /**
   * Implementation of the Word Count Mapper for the WET WARC files from Common Crawl.
   */
  public static class WordCounterMapper extends Mapper<Text, ArchiveReader, Text, LongWritable> {

    /**
     * Overrides the default map method and uses the map file reader to read, parse and map the
     * input.
     *
     * @param key           the generated Hadoop key
     * @param archiveReader the passed archive reader
     * @param context       the passed context
     */
    @Override
    public void map(Text key, ArchiveReader archiveReader, Context context) {
      WordCountV2MapFacade wordCountV2MapFacade = new WordCountV2MapFacade(archiveReader, context);
      wordCountV2MapFacade.mapRecord();
    }
  }
}

