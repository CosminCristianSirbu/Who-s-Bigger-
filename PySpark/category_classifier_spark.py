import os
import re
import sys

import nltk
from pyspark.ml.classification import OneVsRestModel, OneVsRest, LinearSVC
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import HashingTF, IDF, IDFModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, monotonically_increasing_id, regexp_replace
from pyspark.sql.types import StructType, StringType, ArrayType, StructField, BooleanType

from bio_info_boxes_loader import load_info_boxes

# nltk.download('stopwords')
# nltk.download('punkt')
# nltk.download('wordnet')

info_boxes = load_info_boxes()
wiki_clean_regex = re.compile(r'http\S+|<.*?>*?</.*?>|{{.*?}}|[^a-zA-Z]')
alphabetic_regex = re.compile('[a-zA-Z]')


@udf(returnType=ArrayType(StringType()))
def tokenize_and_lemmatize(text):
    """
    Tokenize and lemmatize the data.
    :param text: the data do be processed
    :return: the lemmatized and tokenized data
    """
    from nltk.stem import WordNetLemmatizer

    tokens = [word.lower() for sent in nltk.sent_tokenize(text) for word in nltk.word_tokenize(sent)]
    filtered_tokens = []
    for token in tokens:
        if re.search(alphabetic_regex, token):
            filtered_tokens.append(token)

    lemmatizer = WordNetLemmatizer()
    lem = [lemmatizer.lemmatize(t) for t in filtered_tokens]
    return lem


@udf(returnType=StringType())
def clean_text(text):
    """
    Cleans the text of any markup, links and non-alphabetic characters.
    :param text: the text to be cleaned
    :return: the cleaned text
    """
    text = re.sub(wiki_clean_regex, " ", text)
    text = ' '.join([word for word in text.split() if len(word) > 1])
    text = text.lower()
    return text


@udf(returnType=ArrayType(StringType()))
def remove_stopwords(text):
    """
    Removes all the stop words from the data.
    :param text: the data to be processed
    :return: the data without stop words
    """
    from nltk.corpus import stopwords
    stop_words = set(stopwords.words('english'))
    no_stopword_text = [w for w in text.split() if w not in stop_words]
    return ' '.join(no_stopword_text)


@udf(returnType=BooleanType())
def check_is_biography(wiki_page):
    """
    Checks if a Wiki page is biographical by checking if it's infobox is of the
    biographical type.
    :param wiki_page: the wiki page to be checked
    :return: True if the page is biographical, False otherwise
    """
    is_biography = False
    wiki_page = str(wiki_page)
    for info_box in info_boxes:
        if str("{{" + info_box) in wiki_page:
            is_biography = True
            break
    return is_biography


@udf(returnType=StringType())
def process_wiki_data(wiki_page):
    """
    Processes a wiki article page, removing its notes and citations section.
    :param wiki_page: the raw wiki page dump
    :return: the processed page
    """
    wiki_page = str(wiki_page)
    if "--Notes--" in wiki_page:
        wiki_page = wiki_page[0:wiki_page.index("--Notes--")]
    return wiki_page


class CategoryClassifier:
    """
    Spark Semantic Category Classifier. It is trained on a dataset of wikipedia texts and labels
    and is used to predict the semantic categories of Wikipedia pages.
    """

    def __init__(self, spark_session, input_path, output_path, execution_type, model_path):
        """
        Constructor of the class.
        Initializes the variables.
        :param spark_session: the current Spark session
        :param input_path: the input path for the model
        :param output_path: the output path for the category predictions
        :param execution_type: the type of execution (train/test)
        :param model_path: the path of the model
        """
        self._input_path = input_path
        self._output_path = output_path
        self._spark = spark_session
        self._execution_type = execution_type
        self._data = None
        self._categories_ids = None
        self._model = None
        self._idf_model = None
        self._train_df = None
        self._test_df = None
        self._model_path = model_path
        self._wiki_schema = StructType([
            StructField("title", StringType(), True),
            StructField("revision", StructType([
                StructField("text", StringType(), True)
            ]), True)
        ])
        self._train_schema = StructType([
            StructField("category", StringType(), True),
            StructField("text", StringType(), True)
        ])

    def run(self):
        """
        Runs the mode based on execution type.
        :return: None
        """
        if self._execution_type == "train":
            self._create_model()
        elif self._execution_type == "test":
            self._model_predict()
        else:
            raise Exception("Argument execution_type (args[1]) can only be (train/test)")

    def _load_trained_model(self):
        """
        Loads the already trained model and the data.
        :return: None
        """
        self._data = self._spark \
            .read \
            .format("xml") \
            .option("rowTag", "page") \
            .schema(self._wiki_schema) \
            .load(self._input_path)

        self._data = self._data \
            .withColumn('text', process_wiki_data(col('revision'))) \
            .withColumn('is_biography', check_is_biography(col('revision'))) \
            .filter(col('is_biography') == True) \
            .withColumn('title', regexp_replace(col('title'), ",", " ")) \
            .select("text", "title") \
            .cache()

        self._categories_ids = self._spark \
            .read \
            .parquet(self._model_path + "/categories_ids.parquet")

        self._model = OneVsRestModel \
            .load(self._model_path) \
            .setPredictionCol('category_id')

        self._idf_model = IDFModel \
            .load(self._model_path + "/idf_model")

    def _load_data(self):
        """
        Loads data for training the model.
        :return: None
        """
        self._data = self._spark \
            .read \
            .format("csv") \
            .option("header", True) \
            .schema(self._train_schema) \
            .load(self._input_path)

        self._categories_ids = self._data \
            .select("category") \
            .distinct() \
            .coalesce(1) \
            .withColumn("category_id", monotonically_increasing_id())

        self._data = self._data \
            .join(self._categories_ids, "category") \
            .dropDuplicates(["text", "category"]) \
            .cache()

    def _pre_process_data(self):
        """
        Applies the pre-processing pipeline on the input data.
        :return: None
        """
        self._data = self._data \
            .withColumn('pre_processed_data', clean_text(col('text'))) \
            .withColumn('pre_processed_data', remove_stopwords(col('pre_processed_data'))) \
            .withColumn('pre_processed_data', tokenize_and_lemmatize('pre_processed_data')) \
            .cache()

    def _dataset_split(self):
        """
        Splits the datasets into a training and testing dataframe.
        :return: None
        """
        self._train_df, self._test_df = self._data \
            .select('category_id', 'tf') \
            .randomSplit(weights=[0.80, 0.20], seed=77)

    def _tf_transformation(self):
        """
        Applies the term frequency tranformation on the dataframe.
        :return: None
        """
        hashing_tf = HashingTF(
            inputCol='pre_processed_data',
            outputCol='tf')
        self._data = hashing_tf \
            .transform(self._data) \
            .cache()

    def _train_tf_idf_model(self):
        """
        Trains the TF-IDF model on the training corpora.
        :return: None
        """
        idf = IDF(
            inputCol='tf',
            outputCol='tfidf')
        self._idf_model = idf.fit(self._train_df)

    def _tf_idf_transformation(self):
        """
        Transforms the TF vector to a TF-IDF.
        :return: None
        """
        if self._execution_type == 'train':
            self._train_df = self._idf_model \
                .transform(self._train_df) \
                .cache()
            self._test_df = self._idf_model \
                .transform(self._test_df) \
                .cache()
        elif self._execution_type == 'test':
            self._data = self._idf_model \
                .transform(self._data) \
                .cache()
        else:
            raise Exception("Argument execution_type (args[2]) can only be (train/test)")

    def _check_accuracy(self):
        """
        Checks the accuracy of the model.
        :return: None
        """
        evaluator = MulticlassClassificationEvaluator(
            metricName="accuracy",
            predictionCol="prediction",
            labelCol="category_id")
        predictions = self._model.transform(self._test_df)
        print("Accuracy: {}".format(evaluator.evaluate(predictions)))

    def _train_model(self):
        """
        Trains the model on the training dataframe.
        :return: None
        """
        lsvc = LinearSVC(maxIter=10, regParam=0.5)
        ovr = OneVsRest(
            classifier=lsvc,
            predictionCol="prediction",
            featuresCol="tfidf",
            labelCol="category_id",
            parallelism=2)
        self._model = ovr.fit(self._train_df)

    def _save_model(self):
        """
        Saves the trained model and TF-IDF and the categories ids dataframe.
        :return: None
        """
        self._model \
            .write() \
            .overwrite() \
            .save(self._model_path)
        self._idf_model \
            .write() \
            .overwrite() \
            .save(self._model_path + "/idf_model")
        self._categories_ids \
            .write \
            .parquet(path=self._model_path + "/categories_ids.parquet", mode="overwrite")

    def _predict(self):
        """
        Runs the trained model on new data to predict their semantic category.
        :return: None
        """
        predictions = self._model.transform(self._data)
        predictions = predictions \
            .join(self._categories_ids, "category_id") \
            .select("title", "category")
        predictions \
            .write \
            .csv(self._output_path, mode="overwrite")

    def _create_model(self):
        """
        Pipeline to create a new model.
        :return: None
        """
        self._load_data()
        self._pre_process_data()
        self._tf_transformation()
        self._dataset_split()
        self._train_tf_idf_model()
        self._tf_idf_transformation()
        self._train_model()
        self._save_model()
        self._check_accuracy()

    def _model_predict(self):
        """
        Pipeline to use the trained model to predict semantic categories for new data.
        :return:
        """
        self._load_trained_model()
        self._pre_process_data()
        self._tf_transformation()
        self._tf_idf_transformation()
        self._predict()


def main():
    """
    Driver for the category classifier model.
    It needs the maven package: com.databricks:spark-xml_2.11:0.12.0 (can use different version).
    To use it, the --packages com.databricks:spark-xml_2.11:0.12.0 param has to be added to the spark-submit job.

    It also needs the virtual environment pyspark_env.tar.gz to be shipped with it.
    :return: None
    """

    args = sys.argv
    execution_type = args[1] if len(args) > 1 else ""
    deploy_mode = ""
    model_path = ""
    input_path = ""
    output_path = ""
    spark = None

    if execution_type == "train":
        if len(args) != 5:
            raise Exception("There must be 5 command line arguments: pyspark file, execution_type, " +
                            " deploy_mode (cluster/local), model path, input path")
        deploy_mode = args[2]
        model_path = args[3]
        input_path = args[4]
    elif execution_type == "test":
        if len(args) != 6:
            raise Exception("There must be 6 command line arguments: pyspark file, execution_type, " +
                            " deploy_mode (cluster/local), model path, input path, output_path")
        deploy_mode = args[2]
        model_path = args[3]
        input_path = args[4]
        output_path = args[5]
    else:
        raise Exception("Argument execution_type (args[1]) can only be (train/test)")

    if deploy_mode == "cluster":
        os.environ['PYSPARK_PYTHON'] = "./environment/bin/python"
        spark = SparkSession.builder \
            .master("yarn") \
            .appName("SparkCategoryClassifier") \
            .config("spark.sql.shuffle.partitions", "3") \
            .config("spark.executor.cores", "3") \
            .config("spark.executor.memory", "20g") \
            .config("spark.executor.instances", "10") \
            .config("spark.yarn.dist.archives", "pyspark_env.tar.gz#environment") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

    elif deploy_mode == "local":
        spark = SparkSession.builder \
            .master("local") \
            .appName("SparkCategoryClassifier") \
            .config("spark.sql.shuffle.partitions", "1") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("INFO")

    category_classifier = CategoryClassifier(spark, input_path, output_path, execution_type, model_path)
    category_classifier.run()


if __name__ == "__main__":
    main()
