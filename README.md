# Massive Scale Data Analysis with Hadoop & Spark

The aim of this project is to learn and apply advanced Big Data Analysis Techniques and corresponding frameworks.

The project implements the following problems:

- <b>Word Count</b>: word frequency for a large number of website data (there are 2 versions
  of this problem implemented).
- <b>Distributed Word Grep</b>: the occurrences of a regex expression in a large number of websites
- <b>Naive Page Rank</b>: from principles implementation of the page rank problem using the number of 
  iterations as a metric rather than convergence.
- <b>Page Rank</b>: implementation of the page rank algorithm using the Spark GrapX library that parses
  the entire Wikipedia datasource, creates the page rank graph and runs the page rank algorithm in
  order to rank all the biographical pages.
- <b>Linear Text Semantic Classifier</b>: text semantic classifier build in python with scikitlearn, nltk and 
  pandas.
- <b>Spark Text Semantic Classifier</b>: text semantic classifier built with python, Spark, nltk and SparkMK used to
  predict the semantic category of every biographical page on Wikipedia.
- <b>Historical Significance</b>: program that used the outputs of Page Rank and Spark Text Semantic Classifier
  to create the historical significance scores for all the biographical Wikipedia pages.

## Required Tech Stack

The following software are required to be installed and set-up to run this project.

- Hadoop 3.1.1
- Spark 2.3.2
- HDFS 
- Java 8
- Maven
- Gitlab CI/CD
- Docker
- Conda
- Python 3

## How to generate jars and create conda environment?

To create jars for the Word Count V1, Word Count V2, Distributed Word Grep, Naive Page, Page Rank problems:
- run mvn package

To create conda environment for Spark Text Semantic Classifier and Historical Significance:
- conda create -y -n pyspark_conda_env -c conda-forge pyarrow pandas pyspark nltk conda-pack
- conda activate pyspark_conda_env
- nltk.download('stopwords')
- nltk.download('punkt')
- nltk.download('wordnet')
- Save nltk downloads directory in the py_spark_conda_env.
- conda pack -f -o pyspark_conda_env.tar.gz

## How to get input files?

- Word Count V1: any text file
- Word Count V2, Word Grep: any kind of warc .gz file: download from Common Crawl https://commoncrawl.org/the-data/get-started/
- Text Classifiers training data: provided in PySpark/training_data/labeled_categories.csv
- Naive Page Rank: create text file where each line has 2 words separated by a space. Each word is a vertex and a line denotes a connection between the vertices. 
- Page Rank, Spark Text Semantic Classifier: download Wikipedia data from https://en.wikipedia.org/wiki/Wikipedia:Database_download and https://archive.org/search?query=subject%3A%22enwiki%22%20AND%20subject%3A%22data%20dumps%22%20AND%20collection%3A%22wikimediadownloads%22
- Trained Linear Semantic Classifier input for testing: csv file with a single column with the header text, each line is a raw text for which you want the semantic prediction.

### Where are the output files?

- The output files for all the implemented programs are in the output folder.

## How to run the programs?

The programs can be run into two modes:

- **Local Mode** (pseudo-distributed mode)
- **Distributed Mode**

In case you want to run the programs in Distributed Mode, please mind they are designed
for a 1 master node - 10 data nodes cluster (24GB RAM / node and 3 cores / node) with a block size of 120MB.

The cluster on which these programs were developed: **RHUL Big Data Cluster**.

### Word Count V1

- hadoop jar ./out/artifacts/WordCountV1/WordCountV1.jar deploy_mode input_directory output_directory
- deploy_mode: distributed/local
- input_directory: local directory (local mode) or HDFS directory (distributed mode)
- output_directory: local directory (local mode) or HDFS directory (distributed mode)

### Word Count V2

- hadoop jar ./out/artifacts/WordCountV2/WordCountV2.jar deploy_mode input_directory output_directory
- deploy_mode: distributed/local
- input_directory: local directory (local mode) or HDFS directory (distributed mode)
- output_directory: local directory (local mode) or HDFS directory (distributed mode)

### Distributed Word Grep

- hadoop jar ./out/artifacts/WordGrep/WordGrep.jar deploy_mode input_directory output_directory regex_to_search
- deploy_mode: distributed/local
- input_directory: local directory (local mode) or HDFS directory (distributed mode)
- output_directory: local directory (local mode) or HDFS directory (distributed mode)
- regex_to_search: the regex to search

### Naive Page Rank

- spark-submit ./out/artifacts/NaivePageRank/NaivePageRank.jar input_directory output_directory
- input_directory: local directory (local mode) or HDFS directory (distributed mode)
- output_directory: local directory (local mode) or HDFS directory (distributed mode)

### Page Rank

- spark-submit ./out/artifacts/PageRank/PageRank.jar deploy_mode input_directory output_directory
- deploy_mode: cluster/local
- input_directory: local directory (local mode) or HDFS directory (distributed mode)
- output_directory: local directory (local mode) or HDFS directory (distributed mode)

### Linear Text Semantic Classifier 

- cd PySpark

#### Train Mode

- py category_classifier.py train model_path training_data_path

#### Optimize Mode

- py category_classifier.py optimize training_data_path

#### Test Mode

- py category_classifier.py test model_path input_path output_path

#### Command Line Arguments

- execution_mode: train/optimize/test
- model_path: the local path where the model will be saved/loaded from
- training_data_path: the local path of the training data
- input_path: the local input path for the model to make predictions on
- output_path: the local output path where the model will save the predictions

### Spark Text Semantic Classifier

- cd PySpark

#### Train Mode

- spark-submit --packages com.databricks:spark-xml_2.11:0.12.0 category_classifier_spark.py train deploy_mode model_path training_data_path

#### Test Mode

- spark-submit --packages com.databricks:spark-xml_2.11:0.3.5 category_classifier_spark.py test deploy_mode model_path input_path output_path

#### Command Line Arguments

- execution_mode: train/test
- deploy_mode: cluster/local
- model_path: directory where the model is saved or loaded from (local directory for local mode, HDFS directory for cluster mode)
- training_data_path: the path of the training data (local directory for local mode, HDFS directory for cluster mode)
- input_path: the input path for the model to make predictions on (local directory for local mode, HDFS directory for cluster mode)
- output_path: the output path where the model will save the predictions (local directory for local mode, HDFS directory for cluster mode)

### Historical Significance

- cd PySpark
- spark-submit historical_significance.py deploy_mode page_ranks_path text_semantic_predictions_path output_path
- deploy_mode: local/cluster
- page_ranks_path: the path of the page rank results (local directory for local mode, HDFS directory for cluster mode)
- text_semantic_predictions: the path of the text semantic predictions results (local directory for local mode, HDFS directory for cluster mode)
- output_path: the output path where the historical scores will be saved (local directory for local mode, HDFS directory for cluster mode)

## Jira Integration

This project is plugged in with the following Jira project:

https://cosminsirbu-finalyearproject.atlassian.net/jira/software/c/projects/FYP/boards/1

The Jira issues can be imported from Jira and kept in sync.

All commits that have the jira issue key in the message will
appear in the Jira issue view as well.

## Gitlab CI/CD

This project has an CI/CD pipeline set up with an active runner set up on
zhac254 personal machine.

The pipeline will be triggered by pushes, releases and merges.