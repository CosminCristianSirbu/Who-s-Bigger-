package rhul.ac.uk.wordcountv1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;
import rhul.ac.uk.utils.MapReduceDriver;

/**
 * Driver for the word count problem v1 which only works with raw text data.
 *
 * @author Cosmin Sirbu
 */
public class WordCountV1Driver extends MapReduceDriver {

  /**
   * Main executable of the program. Configures the Hadoop env and runs the program
   * using the tool runner.
   *
   * @param args the input and output directories
   * @throws Exception any Exception caught during runtime
   */
  public static void main(String[] args) throws Exception {
    PropertyConfigurator.configure("src/main/resources/log4j.properties");
    ToolRunner.run(new WordCountV1Driver(), args);
  }

  /**
   * Main runnable of the WordCount problem.
   * Initializes the job and configures it.
   *
   * @param args the input and output file directories
   * @return 0 if the runnable execute correctly
   * @throws Exception any kind of exception caught during runtime
   */
  public int run(String[] args) throws Exception {
    Configuration configuration = createBasicConfiguration(args);
    Job job = Job.getInstance(configuration);
    job.setNumReduceTasks(1);
    setPaths(job, args);

    //set up all the input and output format classes
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    //set up all the mapper and reducer classes
    job.setJarByClass(WordCountV1Driver.class);
    job.setMapperClass(WordCountV1Map.class);
    job.setCombinerClass(WordCountV1Reducer.class);
    job.setReducerClass(WordCountV1Reducer.class);

    setExitCode((job.waitForCompletion(true) ? 0 : 1));
    return exitCode;
  }
}