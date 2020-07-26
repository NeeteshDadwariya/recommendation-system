from pyspark.mllib.recommendation import ALS
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Model:
    def __init__(self, ratings_df):
        self.rating_df = ratings_df
        self.model = None
        self.model_params = None;

    def create_model(self):
        train_data, test_data = self.rating_df.randomSplit([0.8, 0.2])
        print('train_data= ', train_data.head(5))
        print('test_data= ', test_data.head(5))

        self.model_params = {
            'maxIter': 10,
            'regParam': 0.1,
            'rank': 8
        }
        als = ALS(maxIter=self.model_params.maxIter,
                  regParam=self.model_params.regParam,
                  rank=self.model_params.rank,
                  nonnegative=True, coldStartStrategy="drop", userCol='userId',
                  itemCol='movieId', ratingCol='rating')
        self.model = als.fit(train_data)

    def save_model(self):
        # Save the model to S3 / Google drive with the hyper-parameters.
        return
