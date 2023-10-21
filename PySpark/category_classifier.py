import os
import pickle
import re
import shutil
import sys

import nltk
import pandas as pd
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import accuracy_score, f1_score
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.multiclass import OneVsRestClassifier
from sklearn.svm import LinearSVC

# nltk.download('stopwords')
# nltk.download('punkt')
# nltk.download('wordnet')

wiki_clean_regex = re.compile(r'http\S+|<.*?>*?</.*?>|{{.*?}}|[^a-zA-Z]')
alphabetic_regex = re.compile('[a-zA-Z]')


def tokenize_and_lemmatize(text):
    """
    Tokenize and lemmatize the data.
    :param text: the data do be processed
    :return: the lemmatized and tokenized data
    """
    tokens = [word.lower() for sent in nltk.sent_tokenize(text) for word in nltk.word_tokenize(sent)]
    filtered_tokens = []
    for token in tokens:
        if re.search(alphabetic_regex, token):
            filtered_tokens.append(token)

    lemmatizer = WordNetLemmatizer()
    lem = [lemmatizer.lemmatize(t) for t in filtered_tokens]
    return lem


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


def remove_stopwords(text):
    """
    Removes all the stop words from the data.
    :param text: the data to be processed
    :return: the data without stop words
    """
    stop_words = set(stopwords.words('english'))
    no_stopword_text = [w for w in text.split() if w not in stop_words]
    return ' '.join(no_stopword_text)


class CategoryClassifier:
    """
    Linear Semantic classifier model.It is trained on a dataset of wikipedia texts and labels
    and is used to predict the semantic categories of Wikipedia pages.
    """

    def __init__(self, execution_type, input_path, output_path, model_path):
        """
        Constructor of the class.
        Initializes the variables.
        :param execution_type: the type of execution (train/test/optimize)
        :param input_path: the input path
        :param output_path: the output path of the semantic categories predictions
        :param model_path: the model path
        """
        self._input_path = input_path
        self._output_path = output_path
        self._execution_type = execution_type
        self._model = None
        self._tfidf_vec = None
        self._data = None
        self._id_to_category = None
        self._model_path = model_path
        self._x_train = None
        self._x_test = None
        self._y_train = None
        self._y_test = None
        self._x_train_tfidf = None
        self._x_test_tfidf = None

    def _load_trained_model(self):
        """
        Loads the trained model.
        :return: None
        """
        if os.path.exists(self._model_path):
            self._model = pickle.load(open(self._model_path + "/category_classifier.pickle", "rb"))
            self._tfidf_vec = pickle.load(open(self._model_path + "/tfidf.pickle", "rb"))
            self._id_to_category = pickle.load(open(self._model_path + "/categories.pickle", "rb"))
            self._data = pd.read_csv(self._input_path)
        else:
            raise Exception("Model doesn't exist! Please first create the model first")

    def _load_data(self):
        """
        Loads the data to train new model.
        :return: None
        """
        self._data = pd.read_csv(self._input_path)
        self._data['category_id'] = self._data['category'].factorize()[0]
        self._data.drop_duplicates(subset=['category', 'text'], inplace=True)
        category_id_df = self._data[['category', 'category_id']] \
            .drop_duplicates() \
            .sort_values('category_id')
        self._id_to_category = dict(category_id_df[['category_id', 'category']].values)

    def _pre_process_data(self):
        """
        Pre-processes the data in order to be used by the TF-IDF
        :return:
        """
        self._data["clean_text"] = self._data["text"] \
            .apply(clean_text) \
            .apply(remove_stopwords)

    def _dataset_split(self):
        """
        Splits the dataset into a training and testing dataset.
        :return: None
        """
        x = self._data.loc[:, 'clean_text']
        y = self._data.loc[:, 'category_id']
        self._x_train, self._x_test, self._y_train, self._y_test = train_test_split(x, y, test_size=0.2,
                                                                                    random_state=55)

    def _train_tfidf_model(self):
        """
        Trains the TF-IDF model on the training data.
        :return: None
        """
        self._tfidf_vec = TfidfVectorizer(
            stop_words='english',
            ngram_range=(1, 2),
            tokenizer=tokenize_and_lemmatize,
            use_idf=True) \
            .fit(self._x_train)

    def _tfidf_transformation(self):
        """
        Transforms the data using the train TF_IDF.
        :return: None
        """
        if self._execution_type == "train" or self._execution_type == "optimize":
            self._x_train_tfidf = self._tfidf_vec.transform(self._x_train)
            self._x_test_tfidf = self._tfidf_vec.transform(self._x_test)
        elif self._execution_type == "test":
            self._x_test_tfidf = self._tfidf_vec.transform(self._data["clean_text"])

    def _check_accuracy(self):
        """
        Checks the accuracy of the model.
        :return: None
        """
        y_pred = self._model.predict(self._x_test_tfidf)
        accuracy = round(accuracy_score(self._y_test, y_pred) * 100, 2)
        print("Accuracy: " + str(accuracy))

    def _train_model(self):
        """
        Trains the model.
        :return: None
        """
        self._model = OneVsRestClassifier(LinearSVC(C=0.5, max_iter=10))
        self._model.fit(self._x_train_tfidf, self._y_train)

    def _grid_search(self):
        """
        Performs a parameter tuning using grid search.
        :return: None
        """
        self._model = OneVsRestClassifier(LinearSVC())
        parameters = {
            "estimator__C": [0.001, 0.01, 0.1, 0.5, 1, 10, 100],
            "estimator__max_iter": [1, 5, 10, 50, 100]
        }
        model_tunning = GridSearchCV(self._model, param_grid=parameters)
        model_tunning.fit(self._x_train_tfidf, self._y_train)

        print("Best Accuracy: " + str(model_tunning.best_score_))
        print("Best Parameters: " + str(model_tunning.best_params_))

    def _save_model(self):
        """
        Saves the trained model.
        :return: None
        """
        if os.path.exists(self._model_path):
            shutil.rmtree(self._model_path)
        os.mkdir(self._model_path)

        pickle.dump(self._model, open(self._model_path + "/category_classifier.pickle", "wb"))
        pickle.dump(self._tfidf_vec, open(self._model_path + "/tfidf.pickle", "wb"))
        pickle.dump(self._id_to_category, open(self._model_path + "/categories.pickle", "wb"))

    def _predict(self):
        """
        Uses the trained model to make semantic categories predictions.
        :return: None
        """
        y_pred = self._model.predict(self._x_test_tfidf)
        self._data['prediction'] = [self._id_to_category[p] for p in y_pred]
        self._data = self._data[['text', 'prediction']]
        self._data.to_csv(self._output_path, header=True)

    def run(self):
        """
        Runs the model.
        :return: None
        """
        if self._execution_type == "train":
            self._create_model()
        elif self._execution_type == "test":
            self._model_predict()
        elif self._execution_type == "optimize":
            self._optimize_model()
        else:
            raise Exception("Argument execution_type (args[1) can only be (train/test/optimize)")

    def _create_model(self):
        """
        Pipeline to create the model.
        :return: None
        """
        self._load_data()
        self._pre_process_data()
        self._dataset_split()
        self._train_tfidf_model()
        self._tfidf_transformation()
        self._train_model()
        self._check_accuracy()
        self._save_model()

    def _model_predict(self):
        """
        Pipeline to use the trained model to make predictions.
        :return: None
        """
        self._load_trained_model()
        self._pre_process_data()
        self._tfidf_transformation()
        self._predict()

    def _optimize_model(self):
        """
        Pipeline to fine tune the model parameters
        :return: None
        """
        self._load_data()
        self._pre_process_data()
        self._dataset_split()
        self._train_tfidf_model()
        self._tfidf_transformation()
        self._grid_search()


def main():
    """
    Main driver for the model.
    Validates command line arguments and runs the model.
    It needs the packages in the requirements.txt and the nltk download packages (see top of the file).
    :return: None
    """

    args = sys.argv
    execution_type = args[1] if len(args) > 1 else ""
    input_path = ""
    output_path = ""
    model_path = ""

    if execution_type == "train":
        if len(args) != 4:
            raise Exception("For execution type train there must be 4 command line arguments: python file,"
                            " execution type, model path, input path")
        model_path = args[2]
        input_path = args[3]
    elif execution_type == "optimize":
        if len(args) != 3:
            raise Exception("For execution type optimize there must be 3 command line arguments: python file,"
                            " execution type, input path")
        input_path = args[2]
    elif execution_type == "test":
        if len(args) != 5:
            raise Exception("There must be 5 command line arguments: python file,"
                            " execution type, model path, input path, output path")
        model_path = args[2]
        input_path = args[3]
        output_path = args[4]
    else:
        raise Exception("Argument execution_type (args[1) can only be (train/test/optimize)")

    category_classifier = CategoryClassifier(execution_type, input_path, output_path, model_path)
    category_classifier.run()


if __name__ == "__main__":
    main()
