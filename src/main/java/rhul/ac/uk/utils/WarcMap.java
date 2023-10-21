package rhul.ac.uk.utils;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;

import java.io.IOException;

/**
 * The base version of the Map to process warc files.
 * Meant to be extended by the specialised classes.
 * Based on https://github.com/commoncrawl/cc-warc-examples
 *
 * @author Cosmin Sirbu
 */
public class WarcMap {
  /**
   * Archive reader that parses the archived format.
   */
  protected final ArchiveReader archiveReader;

  /**
   * The context the mapper writes to.
   */
  protected final Mapper.Context context;

  /**
   * Basic logger for logging reading events.
   */
  protected final Logger log;


  /**
   * The constructor of the File Reader. Initializes the clas variables.
   *
   * @param newArchiveReader the passed archived reader
   * @param newContext       the passed context
   */
  public WarcMap(ArchiveReader newArchiveReader, Mapper.Context newContext) {
    archiveReader = newArchiveReader;
    context = newContext;
    log = Logger.getLogger(WarcMap.class);
  }

  /**
   * Method that parses the input data from the archive record into a String.
   * Meant to be overwritten by the specialised methods.
   *
   * @param record the de-archived record
   */
  protected void parseInputData(ArchiveRecord record) throws IOException {
  }

  /**
   * Method used to map the input data.
   * Meant to be overwritten by the specialised methods.
   *
   * @throws IOException          if the file reading or writing is wrong
   * @throws InterruptedException if file reading in interrupted
   */
  protected void mapInputData(ArchiveRecord record) throws IOException, InterruptedException {

  }

  /**
   * Reads a new site (record), de-archives it, parses its contents, and writes to the context.
   * It also catches any exceptions and logs them using the logger.
   */
  public void mapRecord() {
    for (ArchiveRecord record : archiveReader) {
      try {
        if (isTextFile(record)) {
          updateRecordsInNo();
          parseInputData(record);
          mapInputData(record);
        } else {
          updateNonTextFilesNo();
        }
      } catch (Exception exception) {
        updateExceptions(exception);
      }
    }
  }

  /**
   * Updates in the logger the number of non-plain texts files encountered.
   */
  protected void updateNonTextFilesNo() {
    context
        .getCounter(MapperCounter.NON_PLAIN_TEXT)
        .increment(1);
  }

  /**
   * Updates in the logger the number of sites (records) parsed in by the mapper.
   */
  protected void updateRecordsInNo() {
    context
        .getCounter(MapperCounter.RECORDS_IN)
        .increment(1);
  }

  /**
   * Updates in the logger the number of empty pages encountered.
   */
  protected void updateEmptyPagesNo() {
    context
        .getCounter(MapperCounter.EMPTY_PAGE_TEXT)
        .increment(1);
  }

  /**
   * Updates the number of exceptions encountered while reading the files.
   *
   * @param exception the exception encountered
   */
  protected void updateExceptions(Exception exception) {
    log.error("Caught Exception", exception);
    context
        .getCounter(MapperCounter.EXCEPTIONS)
        .increment(1);
  }

  /**
   * Checks if the file header is of type plain text.
   *
   * @param record the record to be checked
   * @return true if the file is of type text, false otherwise
   */
  protected boolean isTextFile(ArchiveRecord record) {
    return record
        .getHeader()
        .getMimetype()
        .equals("text/plain");
  }

  /**
   * Enum class for storing some info about the mapper job.
   */
  public enum MapperCounter {
    RECORDS_IN, EMPTY_PAGE_TEXT, EXCEPTIONS, NON_PLAIN_TEXT
  }
}

