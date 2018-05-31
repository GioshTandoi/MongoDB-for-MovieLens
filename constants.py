"""This file contains all the constants variables used over the other files"""
from pymongo import MongoClient

movies_collection_name = 'movies'
tags_collection_name = 'tags'
links_collection_name = 'links'
ratings_collection_name = 'ratings'

ID_Field = '_id'
movieId = 'movieId'
title = 'title'
genres = 'genres'

userId = 'userId'
rating_value = 'rating'
timestamp = 'timestamp'

imdbId = 'imdbId'
tmdbId = 'tmdbId'

tag = 'tag'
movieLensId = 'movieLensId'

movies_fields = [ID_Field, title, genres]
movies_field_avg = 'averageRating'
movies_field_count = 'numberOfRatings'
movies_field_tags = 'tags'
movies_tag_inner_field = 'tag'
ratings_fields = [userId, movieId, rating_value, timestamp]
links_fields = [movieLensId, imdbId, tmdbId]
tags_fields = [userId, movieId, tag, timestamp]

moviesLens_db_name = 'moviesDB'
moviesLens_collection_name = 'movies'

RATINGS_SOURCE_FILE = 'data/ratings.csv'
MOVIES_SOURCE_FILE = 'data/movies.csv'
TAGS_SOURCE_FILE = 'data/tags.csv'
LINKS_SOURCE_FILE = 'data/links.csv'

overallRating = "overallRating"
genre = "genre"
occurrences = "occurrences"
total_count = "total_count"

user_level_operations_file = open('queries_results/user_level_operations' + '.txt', 'a')
tags_level_operations_file = open("queries_results/tag_level_operations" + '.txt', 'a')

"""----------------------------------------- CONNECTING TO MONGODB---------------------------------------------
MongoClient() retrieves the mongodb instance of the running server, which, in this case, is the default one, 
running on the localhost on port 27017

client.moviesDB retrieves a reference to a mongodb database instance (moviesDB). If the database doesn't exists it
will be created. Note that Mongodb creates instances lazily, which means that the database instance will be created
only when effectively used. This is how Mongodb works with collections as well."""

client = MongoClient()
MOVIES_DB = client.moviesDB
WIDER_MOVIES_DB = client.widerMoviesDB

