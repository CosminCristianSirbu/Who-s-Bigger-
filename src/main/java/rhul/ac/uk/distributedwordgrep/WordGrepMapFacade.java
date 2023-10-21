package rhul.ac.uk.distributedwordgrep;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import rhul.ac.uk.utils.WarcMap;

import java.io.IOException;

/**
 * Facade for the Word Grep problem map. Performs the map logic and hides it under mapRecord().
 *
 * @author Cosmin Sirbu
 */
public class WordGrepMapFacade extends WarcMap {

  /**
   * The current website url which was read.
   */
  private final Text website;
  /**
   * The read website data into a Text.
   */
  private final Text mappedWebsite;
  /**
   * The read website data into a String.
   */
  private String websiteData;

  /**
   * The constructor of the Word Grep File Reader.
   * Initializes the class variables.
   *
   * @param newArchiveReader the archived reader to be used
   * @param newContext       the context to be used
   */
  public WordGrepMapFacade(
      ArchiveReader newArchiveReader,
      Mapper<org.apache.hadoop.io.Text, org.archive.io.ArchiveReader, org.apache.hadoop.io.Text,
          org.apache.hadoop.io.Text>.Context newContext) {
    super(newArchiveReader, newContext);
    website = new Text();
    mappedWebsite = new Text();
  }

  /**
   * Overrides the WarcMap method.
   * Transforms the de-archived byte array into a String so words can be parsed, removing all
   * end of line characters.
   *
   * @param record the website record read
   * @throws IOException when the record reading or writing is wrong
   */
  @Override
  protected void parseInputData(ArchiveRecord record) throws IOException {
    byte[] rawData = IOUtils.toByteArray(record, record.available());
    websiteData = new String(rawData).replaceAll("[\\r\\n]+", "");
    mappedWebsite.set(websiteData);
  }

  /**
   * Overrides the WarcMap method.
   * Process each site data and if the search word is found within the string, it writes
   * it to the context.
   *
   * @param record the read website record
   * @throws IOException          if the file reading or writing is wrong
   * @throws InterruptedException if file reading in interrupted
   */
  @Override
  protected void mapInputData(ArchiveRecord record) throws IOException, InterruptedException {
    String crawledWebsite = record
        .getHeader()
        .getUrl();
    website.set(crawledWebsite);

    Configuration configuration = context.getConfiguration();
    String searchRegex = configuration.get("searchRegex");

    if (websiteData.length() == 0) {
      updateEmptyPagesNo();
    } else {
      if (websiteData.contains(searchRegex)) {
        context.write(website, mappedWebsite);
      }
    }
  }
}

