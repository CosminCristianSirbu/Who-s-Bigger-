package rhul.ac.uk.distributedwordgrep;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The Reducer implementation for the Distributed Grep problem.
 *
 * @author Cosmin Sirbu
 */
public class WordGrepReducer extends Reducer<Text, Text, Text, Text> {

  /**
   * Class var to store the read & formatted website data.
   */
  private final Text websiteData = new Text();

  /**
   * Class var to store the output value to be written to the context.
   */
  private final Text output = new Text();

  /**
   * An array to store the lines where the regex expression is found.
   */
  private List<String> linesArray;
  /**
   * The search regex to be used for searching through the website data.
   */
  private String searchRegex;
  /**
   * List to store the parsed & split into lines website data.
   */
  private ArrayList<String> lines;
  /**
   * Var to indicate if the reducer is run in a testing environment.
   */
  private boolean testMode = false;

  /**
   * A builder used to construct the regex expression found positions String.
   */
  private StringBuilder positions;

  /**
   * Setter for the searchRegex var.
   *
   * @param newSearchRegex the new value for the search regex
   */
  public void setSearchRegex(String newSearchRegex) {
    searchRegex = newSearchRegex.toLowerCase();
    testMode = true;
  }

  /**
   * Searches for the search regex among the formatted lines and return its found
   * line-offset positions.
   *
   * @param lines       the lines where the word is searched
   * @param searchRegex the search regex input searched for in the lines
   */
  private void getRegexPositions(ArrayList<String> lines, String searchRegex) {
    positions = new StringBuilder();
    ArrayList<String> searchingLines = new ArrayList<>(lines);
    linesArray = new ArrayList<>(Collections.emptyList());
    for (int i = 0; i < searchingLines.size(); i++) {
      String line = searchingLines
          .get(i)
          .toLowerCase();
      searchingLines.set(i, line);
      if (line.contains(searchRegex)) {
        String linePositions = getInLinePositions(line, searchRegex);
        String lineNo = String.valueOf(i + 1);
        positions
            .append("Line ")
            .append(lineNo)
            .append(" - positions: ")
            .append(linePositions);
        linesArray.add(lineNo);
      }
    }
  }

  /**
   * Searches for the search regex among a website line.
   *
   * @param line        the current line to search
   * @param searchRegex the search regex input searched for in the line
   * @return a formatted string with the found positions of the search regex in the input line
   */
  private String getInLinePositions(String line, String searchRegex) {
    int wordFoundIndex = line.indexOf(searchRegex);
    int regexFoundLength = searchRegex.length();
    StringBuilder linePositions = new StringBuilder();

    while (wordFoundIndex >= 0) {
      linePositions
          .append(wordFoundIndex)
          .append(",");
      wordFoundIndex = line.indexOf(searchRegex, wordFoundIndex + regexFoundLength);
    }
    if (regexFoundLength > 0) {
      linePositions = new StringBuilder(linePositions.substring(0, linePositions.length() - 1));
    }
    linePositions.append("\n");
    return linePositions.toString();
  }

  /**
   * Nicely formats the output of where the regex expression is found.
   */
  private void formatOutput() {
    StringBuilder formattedData = new StringBuilder();
    int lineNo;
    for (int i = 0; i < lines.size(); i++) {
      lineNo = i + 1;
      if (linesArray.contains(String.valueOf(lineNo))) {
        formattedData
            .append(lineNo)
            .append(". ")
            .append(lines.get(i))
            .append("\n");
      }
    }
    websiteData.set(formattedData.toString());
    if (positions.length() != 0) {
      output.set("\n" + searchRegex + " found at positions: \n" + positions + "\n" + websiteData);
    }
  }

  /**
   * Splits the input website data into equal sized lines.
   * It also doesn't break end line words.
   *
   * @param websiteData the website data to be split
   */
  private void splitInputIntoLines(String websiteData) {
    int splitIndex = 0;
    int lineLength = 100;
    ArrayList<String> inputLines = new ArrayList<>();
    while (splitIndex < websiteData.length()) {
      String line = "";
      int endIndex = Math.min(splitIndex + lineLength, websiteData.length());

      //the index of the next line space
      int nextSpaceIdx = websiteData
          .substring(endIndex)
          .indexOf(" ");

      //if there is a next line space, cut the line at end of the word
      if (nextSpaceIdx != -1) {
        line = websiteData.substring(splitIndex, endIndex + nextSpaceIdx);
        splitIndex += lineLength + nextSpaceIdx;
      } else {
        line = websiteData.substring(splitIndex, endIndex);
        splitIndex += lineLength;
      }
      inputLines.add(line);
    }
    lines = inputLines;
  }

  /**
   * Performs the reduce step logic for a given record.
   *
   * @param value the value of the website data
   */
  private void reduceRecord(Text value) {
    websiteData.set(value);
    splitInputIntoLines(value.toString());
    getRegexPositions(lines, searchRegex);
    formatOutput();
  }

  /**
   * Receives a partition of the mapped input data holding a website url and the line with the
   * matched regex and formats the output based on the found regex input.
   *
   * @param website the mapped website entry
   * @param values  the list of found word frequency values
   * @param context the context used to reduce the mapped values
   * @throws IOException          when the input is wrong
   * @throws InterruptedException when the context writing is interrupted
   */
  public void reduce(Text website, Iterable<Text> values, Context context) throws IOException,
      InterruptedException {
    if (!testMode) {
      Configuration conf = context.getConfiguration();
      setSearchRegex(conf
          .get("searchRegex")
          .toLowerCase());
    }
    for (Text v : values) {
      reduceRecord(v);
    }
    context.write(website, output);
  }
}
