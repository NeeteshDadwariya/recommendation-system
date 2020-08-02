from pyspark.mllib.recommendation import ALS
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ModelBuilder:
    def __init__(self, ratings_df):
        self.rating_df = ratings_df
        self.model = None
        self.model_params = None;

    def create_model(self):
        train_data, test_data = self.rating_df.randomSplit([0.8, 0.2])
        print('train_data= ', train_data.head(5))
        print('test_data= ', test_data.head(5))

        self.model_params = {'maxIter': 10, 'regParam': 0.1, 'rank': 8}
        als = ALS(maxIter=5, regParam=0.09, rank=25, userCol="reviewerID_index", itemCol="asin_index",
                  ratingCol="overall", coldStartStrategy="drop", nonnegative=True)
        self.model = als.fit(train_data)
        print('hello')

    def save_model(self):
        # Save the model to S3 / Google drive with the hyper-parameters.
        return
