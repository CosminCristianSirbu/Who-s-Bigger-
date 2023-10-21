package rhul.ac.uk.utils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.archive.io.ArchiveReader;

/**
 * Implementation of FileInputFormat for WARC files.
 * Doesn't allow Hadoop to split the compressed files.
 *
 * @author Cosmin Sirbu
 */
public class WarcFileInputFormat extends FileInputFormat<Text, ArchiveReader> {

  /**
   * Creates a new Record Reader for WARC file and returns it.
   *
   * @param split   the split to be read
   * @param context the information about the task
   * @return a WARC file record reader instance
   */
  @Override
  public RecordReader<Text, ArchiveReader> createRecordReader(
      InputSplit split, TaskAttemptContext context) {
    return new WarcFileRecordReader();
  }

  /**
   * Since the files processed are compressed with gzip, they cannot be split.
   *
   * @param context  the job context
   * @param filename the file name to check
   * @return false, not letting Hadoop split the compressed files.
   */
  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }
}