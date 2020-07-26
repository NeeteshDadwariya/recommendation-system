import gdown
import os
import findspark
import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ROOT_FOLDER = os.path.abspath(os.path.join(os.getcwd(), '../..'))
DATA_FOLDER = ROOT_FOLDER + "/data/"
logger.info("ROOT_FOLDER = %s", ROOT_FOLDER)
logger.info("DATA_FOLDER = %s", DATA_FOLDER)

LINKS_CSV = 'links.csv'
MOVIES_CSV = 'movies.csv'
RATINGS_CSV = 'ratings.csv'
TAGS_CSV = 'tags.csv'

data_links = 'https://drive.google.com/uc?id=19cRdbSbDD4lnKAbv6nfwppRXL7kko6HT'
data_movies = 'https://drive.google.com/uc?id=14s8JDudJHGirQT3VYFwp18JBwSX3liYZ'
data_ratings = 'https://drive.google.com/uc?id=1hYYWUHk5hrDsCdj4BJG0UXBhQ_-AiZWO'
data_tags = 'https://drive.google.com/uc?id=1y7px4xin3_9KBvdAnmiz_uH0uBtPo-hH'

links_df = None
movies_df = None
ratings_df = None
tags_df = None

# Initializing spark environment
logger.info("Initializing spark environment...")
findspark.init()
spark = SparkSession.builder.getOrCreate()
logger.info("Spark environment initialized.")


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
    global links_df, movies_df, ratings_df, tags_df
    links_df = read_csv_to_df(LINKS_CSV)
    movies_df = read_csv_to_df(MOVIES_CSV)
    ratings_df = read_csv_to_df(RATINGS_CSV)
    tags_df = read_csv_to_df(TAGS_CSV)

    # Show data head
    print('RATINGS=: ', ratings_df.head(5), end='\n\n')
    print('MOVIES=: ', movies_df.head(5), end='\n\n')
    print('TAGS=: ', tags_df.head(5), end='\n\n')
    print('LINKS=: ', links_df.head(5), end='\n\n')
