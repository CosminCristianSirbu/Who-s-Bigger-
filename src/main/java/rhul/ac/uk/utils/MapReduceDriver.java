package rhul.ac.uk.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.nio.file.FileSystems;

import static org.apache.hadoop.fs.FileSystem.newInstance;

/**
 * Base MapReduceDriver class to be extended by the specialised MR Drivers.
 *
 * @author Cosmin Sirbu
 */
public abstract class MapReduceDriver extends Configured implements Tool {
  /**
   * The exit code of the program runnable.
   */
  protected int exitCode;

  /**
   * Returns the value of the exit code.
   *
   * @return the program runnable exit code
   */
  public int getExitCode() {
    return exitCode;
  }

  /**
   * Sets the value of the exit code.
   *
   * @param newExitCode the new exit code
   */
  public void setExitCode(int newExitCode) {
    exitCode = newExitCode;
  }

  /**
   * It creates a basic configuration and checks if the job mode type is valid.
   * If job mode is the local file system then it sets the config to the formatted local
   * filesystem.
   *
   * @param args the command line arguments
   * @return the initialised job
   * @throws Exception if the job type is not valid
   */
  public Configuration createBasicConfiguration(String[] args) throws Exception {
    String jobMode = args[0];
    Configuration configuration = new Configuration();
    configuration.set("fs.hdfs.impl",
        org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

    if (jobMode.equals("local") || jobMode.equals("distributed")) {
      if (jobMode.equals("local")) {
        String localDir = "file:///" + FileSystems
            .getDefault()
            .getPath("")
            .toAbsolutePath()
            .toString()
            .replaceAll(" ", "%20")
            .replaceAll("\\\\", "/");
        configuration.set("fs.default.name", localDir);
        configuration.set("mapreduce.framework.name", "local");
      }
    } else {
      throw new Exception(
          "The job mode specified is not valid! (options are local or " + "distributed)");
    }
    return configuration;
  }

  /**
   * Depending on the job mode type, it deletes the output path if it exists, checks for any
   * configuration errors and formats the job paths.
   *
   * @param job  the initialized job
   * @param args the command line arguments
   * @throws Exception if there is a problem reading the hadoop base file system.
   */
  public void setPaths(Job job, String[] args) throws Exception {
    Path hadoopInputPath = new Path(args[1]);
    Path hadoopOutputPath = new Path(args[2]);

    try (FileSystem fs = newInstance(job.getConfiguration())) {
      if (!fs.exists(hadoopInputPath)) {
        throw new Exception("Specified input path doesn't exist!");
      }
      if (fs.exists(hadoopOutputPath)) {
        fs.delete(hadoopOutputPath, true);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    //set up the input and output paths
    FileInputFormat.addInputPath(job, hadoopInputPath);
    FileOutputFormat.setOutputPath(job, hadoopOutputPath);
  }
}
