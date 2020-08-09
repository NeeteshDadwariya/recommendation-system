from fastapi import FastAPI, HTTPException
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import logging
import findspark
import gdown
import uvicorn
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Export app to start uvicorn server
app = FastAPI()

# Initialize spark environment
logger.info("Initializing spark environment...")
findspark.init()
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
logger.info("Spark environment initialized.")

# Load saved model from inference pipeline
logger.info("Loading model from model_dumps...")
ROOT_FOLDER = os.getcwd()
MODEL_DUMP_FOLDER = ROOT_FOLDER + '/model_dumps/'
MODEL_FILE_NAME = 'als_matrix_factorization.pkl'
logger.info('MODEL_DUMP_FOLDER = %s', MODEL_DUMP_FOLDER)
model = ALSModel.load(MODEL_DUMP_FOLDER + MODEL_FILE_NAME)
logger.info("Model successfully loaded!!")

# Loading dataset
DATA_FOLDER = ROOT_FOLDER + "/data/"
LINKS_CSV = 'links.csv'
MOVIES_CSV = 'movies.csv'
RATINGS_CSV = 'ratings.csv'
TAGS_CSV = 'tags.csv'
data_links = 'https://drive.google.com/uc?id=19cRdbSbDD4lnKAbv6nfwppRXL7kko6HT'
data_movies = 'https://drive.google.com/uc?id=14s8JDudJHGirQT3VYFwp18JBwSX3liYZ'
data_ratings = 'https://drive.google.com/uc?id=1hYYWUHk5hrDsCdj4BJG0UXBhQ_-AiZWO'
data_tags = 'https://drive.google.com/uc?id=1y7px4xin3_9KBvdAnmiz_uH0uBtPo-hH'


def download_from_drive(url, output_filename):
    output_filepath = DATA_FOLDER + output_filename
    if not os.path.isfile(output_filepath):
        gdown.download(url, output_filepath, quiet=True)


def read_csv_to_df(filename):
    return spark.read.option("header", "true").csv(DATA_FOLDER + filename)


def load_data():
    # Download the dataset from the google drive so as to avoid large files in github.
    download_from_drive(data_links, LINKS_CSV)
    download_from_drive(data_movies, MOVIES_CSV)
    download_from_drive(data_ratings, RATINGS_CSV)
    download_from_drive(data_tags, TAGS_CSV)

    # Read date files from spark context
    links_df = read_csv_to_df(LINKS_CSV)
    movies_df = read_csv_to_df(MOVIES_CSV)
    ratings_df = read_csv_to_df(RATINGS_CSV)
    tags_df = read_csv_to_df(TAGS_CSV)

    return links_df, movies_df, ratings_df, tags_df


links_df, movies_df, ratings_df, tags_df = load_data()


@app.get("/get-recommended-users-for-movie/{movie_name}/{no_of_users}")
def get_recommended_users_for_movie(movie_name: str, no_of_users: int):
    movieId_RDD = movies_df.where(col('title') == movie_name).select("movieId").rdd.flatMap(lambda x: x)
    if not movieId_RDD.isEmpty():
        movieId = int(movieId_RDD.first())
        recommendations_df = model.recommendForItemSubset(spark.createDataFrame([(movieId,)], ['movieId']), no_of_users)
        recommended_users = (recommendations_df.select('recommendations').rdd
                             .map(lambda x: x[0])
                             .flatMap(lambda x: x)
                             .map(lambda x: [x.userId, round(x.rating, 2)])
                             .toDF(['userId', 'rating']))
        return recommended_users.toJSON().map(lambda j: json.loads(j)).collect()
    else:
        raise HTTPException(status_code=404, detail="Movie '{}' not present in dataset.".format(movie_name))


@app.get("/get-recommended-movies-for-user/{user_id}/{no_of_movies}")
def get_recommended_users_for_movie(user_id: str, no_of_movies: int):
    recommended_movies_df = model.recommendForUserSubset(spark.createDataFrame([(user_id,)], ['userId']), no_of_movies)
    if not recommended_movies_df.rdd.isEmpty():
        recommended_movies = (recommended_movies_df.select('recommendations').rdd
                              .map(lambda x: x[0])
                              .flatMap(lambda x: x)
                              .map(lambda x: [x.movieId, round(x.rating, 2)])
                              .toDF(['movieId', 'rating']))
        result_df = recommended_movies.join(movies_df, on=['movieId'], how='left_outer').select('movieId', 'title',
                                                                                                'genres', 'rating')
        return result_df.toJSON().map(lambda j: json.loads(j)).collect()

    else:
        raise HTTPException(status_code=404, detail="userId '{}' not present in dataset.".format(user_id))


# TO test individually without make
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
