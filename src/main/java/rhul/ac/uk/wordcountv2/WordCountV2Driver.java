package rhul.ac.uk.wordcountv2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;
import rhul.ac.uk.utils.MapReduceDriver;
import rhul.ac.uk.utils.WarcFileInputFormat;

/**
 * Driver for the WET Word Count implementation using the Common Crawl web scrapped data.
 *
 * @author Cosmin Sirbu
 */
public class WordCountV2Driver extends MapReduceDriver {

  /**
   * Main executable of the program. Configures the Hadoop env and runs the program
   * using the tool runner.
   *
   * @param args the program passed arguments
   * @throws Exception any kind of encountered Exception
   */
  public static void main(String[] args) throws Exception {
    PropertyConfigurator.configure("src/main/resources/log4j.properties");
    ToolRunner.run(new WordCountV2Driver(), args);
  }

  /**
   * Runs the main program. Initializes the job, configures it and runs the MR job.
   *
   * @param args command specific arguments.
   * @return 0 if program terminates successfully, 1 otherwise
   * @throws Exception any kind of encountered Exception
   */
  @Override
  public int run(String[] args) throws Exception {
    Configuration configuration = createBasicConfiguration(args);
    Job job = Job.getInstance(configuration);
    job.setNumReduceTasks(1);
    setPaths(job, args);

    //set up all the input and output format classes
    job.setInputFormatClass(WarcFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    //set mapper and reducer classes
    job.setJarByClass(WordCountV2Driver.class);
    job.setCombinerClass(WordCountV2Reducer.class);
    job.setMapperClass(WordCountV2Map.WordCounterMapper.class);
    job.setReducerClass(WordCountV2Reducer.class);

    //wait for job to finish successfully
    setExitCode((job.waitForCompletion(true) ? 0 : 1));
    return exitCode;
  }
}
