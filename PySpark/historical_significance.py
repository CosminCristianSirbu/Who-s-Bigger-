import sys

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StringType, StructField, DoubleType

"""
Dictionary for mapping categories to their category factor.
"""
category_factors_dict = {
    "entertainment": 0.85,
    "sport": 0.90,
    "military": 0.95,
    "culture & arts": 1.00,
    "philosophy": 1.05,
    "religion": 1.10,
    "politics & leaders": 1.15,
    "science & technology": 1.20
}

"""
UDF to convert a vector back to normal column.
"""
vector_to_values = udf(lambda x: round(float(list(x)[0]), 3), DoubleType())


@udf
def category_to_factor(category):
    """
    Converts the predicted biographical wiki page category to its factor.
    :param category: the category to be converted
    :return: the converted category
    """
    return category_factors_dict.get(category)


class HistoricalSignificance:
    """
    Class to calculate the historical significance of the biographical wiki pages using the
    already calculated page rank scores and the predicted categories.
    """

    def __init__(self, spark, page_ranks_dir, categories_dir, output_path):
        """
        Constructor for the Historical Significance class.
        Initializes its variables.
        :param spark: the current Spark session
        :param page_ranks_dir: the directory of the computed page ranks
        :param categories_dir: the directory of the predicted categories
        :param output_path: the output path for the historical significance scores
        """
        self._page_ranks_dir = page_ranks_dir
        self._categories_dir = categories_dir
        self._output_path = output_path
        self._spark = spark
        self._page_ranks = None
        self._categories = None
        self._historical_significance_df = None
        self._page_ranks_schema = StructType([
            StructField("page", StringType(), True),
            StructField("page_rank", DoubleType(), True)
        ])
        self._categories_schema = StructType([
            StructField("page", StringType(), True),
            StructField("category", StringType(), True)
        ])

    def _calculate_historical_significance(self):
        """
        Calculates the final historical significance score by multiplying the page rank
        and the category factor
        :return: None
        """
        self._historical_significance_df = self._historical_significance_df \
            .withColumn("historical_significance_score",
                        col("category_factor") * col("page_rank"))

    def _save_historical_significance_scores(self):
        """
        Saves the calculated historical significance scores to the output directory.
        :return: None
        """
        self._historical_significance_df \
            .write \
            .csv(path=self._output_path, mode="overwrite", header=True)

    def _normalize_historical_significance_score(self):
        """
        Normalizes the historical significance scores using the Min Max scaler
        so the scores are in range 0-1
        :return: None
        """
        assembler = VectorAssembler(inputCols=['historical_significance_score'],
                                    outputCol='historical_significance_score_vec')

        scaler = MinMaxScaler(inputCol='historical_significance_score_vec',
                              outputCol='historical_significance_score_scaled')

        pipeline = Pipeline(stages=[assembler, scaler])

        self._historical_significance_df = pipeline \
            .fit(self._historical_significance_df) \
            .transform(self._historical_significance_df) \
            .withColumn("historical_significance_score", vector_to_values("historical_significance_score_scaled")) \
            .drop("historical_significance_score_vec", "historical_significance_score_scaled", "category_factor") \
            .orderBy('historical_significance_score', ascending=False)

    def _assemble_historical_significance_dataframe(self):
        """
        Creates the historical significance dataframe by joining the page ranks and
        category predictions dataframes.
        :return: None
        """
        self._historical_significance_df = self._page_ranks \
            .join(self._categories, "page") \
            .withColumn("category_factor", category_to_factor(col("category")))

    def _read_input(self):
        """
        Reads the page rank and predicted categories input files into dataframes.
        :return: None
        """
        self._page_ranks = self._spark \
            .read \
            .format("csv") \
            .schema(self._page_ranks_schema) \
            .load(self._page_ranks_dir)

        self._categories = self._spark \
            .read \
            .format("csv") \
            .schema(self._categories_schema) \
            .load(self._categories_dir)

    def run(self):
        """
        Runs the Historical Significance program.
        :return: None
        """
        self._read_input()
        self._assemble_historical_significance_dataframe()
        self._calculate_historical_significance()
        self._normalize_historical_significance_score()
        self._save_historical_significance_scores()


def main():
    """
    Main entry point for the Historical Significance program.
    Loads and validates command line arguments and runs the program.
    :return: None
    """
    args = sys.argv

    if len(args) != 5:
        raise Exception("There must be 4 command line arguments: pyspark file, deploy_mode" +
                        " (cluster/local), ranks input file, categories input file and output file")

    deploy_mode = args[1]
    ranks_input_dir = args[2]
    categories_input_dir = args[3]
    output_file = args[4]
    master = ""

    if deploy_mode != "cluster" and deploy_mode != "local":
        raise Exception("Argument deploy_mode (args[1]) can only be (cluster/local)")

    if deploy_mode == "local":
        master = "local"
    elif deploy_mode == "cluster":
        master = "yarn"

    spark = SparkSession.builder \
        .master(master) \
        .config("spark.sql.shuffle.partitions", "1") \
        .appName("Historical Significance") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    historical_significance = HistoricalSignificance(spark, ranks_input_dir, categories_input_dir, output_file)
    historical_significance.run()


if __name__ == "__main__":
    main()
