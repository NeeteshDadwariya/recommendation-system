from pyspark.ml.recommendation import ALS
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ModelBuilder:
    def __init__(self, sc, ratings_df):
        self.rating_df = ratings_df
        self.model = None
        self.model_params = None

    def train_model(self):
        logger.info("Training the ALS model...")

        train_data, test_data = self.rating_df.randomSplit([0.8, 0.2])
        print('train_data= ', train_data.head(5))
        print('test_data= ', test_data.head(5))

        self.model_params = {
            'maxIter': 10,
            'regParam': 0.1,
            'rank': 8,
            'seed': 43,
            'iterations': 10,
            'regularization_parameter': 0.1
        }

        self.model = ALS(maxIter=5, regParam=0.09, rank=25, userCol="userId", itemCol="movieId",
                         ratingCol="rating", coldStartStrategy="drop", nonnegative=True);
        self.model.fit(train_data);
        logger.info("ALS model built!")

    def save_model(self):
        # TODO:Save the model to S3 / Google drive with the hyper-parameters.
        return
