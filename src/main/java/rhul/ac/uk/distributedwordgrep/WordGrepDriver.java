package rhul.ac.uk.distributedwordgrep;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;
import rhul.ac.uk.utils.MapReduceDriver;
import rhul.ac.uk.utils.WarcFileInputFormat;

/**
 * The driver for the Word Grep Problem used to run the MR program.
 *
 * @author Cosmin Sirbu
 */
public class WordGrepDriver extends MapReduceDriver {
  /**
   * Main executable of the program. Configures the Hadoop env and runs the program
   * using the tool runner.
   *
   * @param args the program passed arguments
   * @throws Exception any kind of encountered Exception
   */
  public static void main(String[] args) throws Exception {
    PropertyConfigurator.configure("src/main/resources/log4j.properties");
    ToolRunner.run(new WordGrepDriver(), args);
  }

  /**
   * Initializes the job, configures it and runs the MR job.
   *
   * @param args command specific arguments.
   * @return 0 if program terminates successfully, 1 otherwise
   * @throws Exception any kind of encountered Exception
   */
  @Override
  public int run(String[] args) throws Exception {
    Configuration configuration = createBasicConfiguration(args);
    configuration.set("searchRegex", args[3]);
    Job job = Job.getInstance(configuration);
    job.setNumReduceTasks(1);
    setPaths(job, args);

    //set up all the input and output format classes
    job.setInputFormatClass(WarcFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    //set mapper and reducer classes
    job.setMapperClass(WordGrepMap.GrepWordMapper.class);
    job.setReducerClass(WordGrepReducer.class);
    job.setJarByClass(WordGrepDriver.class);

    //wait for job to finish successfully
    setExitCode((job.waitForCompletion(true) ? 0 : 1));
    return exitCode;
  }
}
