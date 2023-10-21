package rhul.ac.uk.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.archive.io.ArchiveReader;
import org.archive.io.warc.WARCReaderFactory;

import java.io.IOException;

/**
 * Processes a single compressed file input amd returns a single WARC Archive Reader.
 * This can contain multiple documents handled by a single mapper.
 *
 * @author Cosmin Sirbu
 */
public class WarcFileRecordReader extends RecordReader<Text, ArchiveReader> {

  /**
   * The archive reader path.
   */
  private String archiveReaderPath;

  /**
   * The archive reader used to process the file.
   */
  private ArchiveReader archiveReader;

  /**
   * the file input stream reader.
   */
  private FSDataInputStream fileInputStream;

  /**
   * Flag that checks if a file was read.
   */
  private boolean fileRead = false;

  /**
   * Initializes the variables.
   *
   * @param inputSplit the split that defines the range of records to read
   * @param context    the information about the task
   * @throws IOException when the input is wrong
   */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
    FileSplit split = (FileSplit) inputSplit;
    Configuration conf = context.getConfiguration();
    Path path = split.getPath();
    FileSystem fs = path.getFileSystem(conf);
    fileInputStream = fs.open(path);
    archiveReaderPath = path.getName();
    archiveReader = WARCReaderFactory.get(path.getName(), fileInputStream, true);
  }

  /**
   * Each file produces only one ArchiveReader.
   * If it was read, there are any other to be read.
   *
   * @return true if the file was read
   */
  @Override
  public boolean nextKeyValue() {
    if (fileRead) {
      return false;
    }
    fileRead = true;
    return true;
  }

  /**
   * Return the path the compressed file uses as a key.
   *
   * @return the archive reader path
   */
  @Override
  public Text getCurrentKey() {
    return new Text(archiveReaderPath);
  }

  /**
   * The only value to return is the output of the compressed file.
   *
   * @return the output of the compressed file stored in archiveReader
   */
  @Override
  public ArchiveReader getCurrentValue() {
    return archiveReader;
  }

  /**
   * Each file produces only one ArchiveReader which will be set to 1 after file was read.
   *
   * @return 1 if the file was read, 0 otherwise
   */
  @Override
  public float getProgress() {
    return fileRead ? 1 : 0;
  }

  /**
   * Closes the file input stream and archive reader.
   *
   * @throws IOException when the input is wrong
   */
  @Override
  public void close() throws IOException {
    fileInputStream.close();
    archiveReader.close();
  }

}