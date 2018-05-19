__author__ = 'Giorgia'

from pymongo import MongoClient
from pymongo.errors import BulkWriteError, ConnectionFailure
import csv
import pprint

"""-----------------------------------------------------CONSTANTS DECLARATION-------------------------- """
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

movies_fields = [ID_Field, title, genres]
movie_field_avg = 'averageRating'
movie_field_count = 'numberOfRatings'
ratings_fields = [userId, movieId, rating_value, timestamp]
links_fields = [movieId, imdbId, tmdbId]
tags_fields = [userId, movieId, tag, timestamp]

RATINGS_SOURCE_FILE = 'data/ratings.csv'
MOVIES_SOURCE_FILE = 'data/movies.csv'
TAGS_SOURCE_FILE = 'data/tags.csv'
LIKS_SOURCE_FILE = 'data/links.csv'

"""----------------------------------------- CONNECTING TO MONGODB---------------------------------------------
MongoClient() retrieves the mongodb instance of the running server, which, in this case, is the default one, 
running on the localhost on port 27017
client.moviesDB retrieves a reference to a mongodb database instance (moviesDB). If the database doesn't exists it
will be created"""

client = MongoClient()
MOVIES_DB = client.moviesDB


MOVIES_DB.ratings.drop()
csvfile = open(RATINGS_SOURCE_FILE, 'r', newline='\n', encoding='utf8')
data = csv.reader(csvfile)
outcome = []

next(data)
for row in data:
    i = 0
    current_document_to_insert = {}
    for field in ratings_fields:
        if field == 'rating':
            try:
                current_document_to_insert[field] = float(row[i])
            except ValueError:
                current_document_to_insert[field] = row[i]
        else:
            try:
                current_document_to_insert[field] = int(row[i])
            except ValueError:
                current_document_to_insert[field] = row[i]

        i += 1
    outcome.append(current_document_to_insert)


MOVIES_DB.ratings.insert_many(outcome)


MOVIES_DB.movies.drop()

csvfile = open(MOVIES_SOURCE_FILE, 'r', newline='\n', encoding='utf8')
data = csv.reader(csvfile)
outcome = []

"""
Deprecated since version 3.4: Mongodb 3.4 deprecates the 
db.collection.group() method. Use db.collection.aggregate() 
with the $group stage or db.collection.mapReduce() instead.
"""


pipeline = [{"$group": {"_id": "$movieId", "averageRating": {"$avg": "$rating"}, "count": {"$sum": 1}}}]
cursor = MOVIES_DB.ratings.aggregate(pipeline, cursor={})

group_result_list = []
for doc in cursor:
    group_result_list.append(doc)


def find(list_of_documents, identifier):
    for d in list_of_documents:
        if d[ID_Field] == identifier:
            return d
    return 0


next(data)
for row in data:
    i = 0
    current_document_to_insert = {}
    for field in movies_fields:
        try:
            current_document_to_insert[field] = int(row[i])
        except ValueError:
            current_document_to_insert[field] = row[i]
        i += 1
    calculated_fields = find(group_result_list, current_document_to_insert[ID_Field])
    if calculated_fields != 0:
        current_document_to_insert[movie_field_avg] = round(calculated_fields['averageRating'], 2)
        current_document_to_insert[movie_field_count] = calculated_fields['count']

    outcome.append(current_document_to_insert)

try:
    MOVIES_DB.movies.insert_many(outcome)
except BaseException as bwe:
        print(bwe.details)


"""-------------------------LOADING TAGS COLLECTION-------------------------------------------"""

MOVIES_DB.tags.drop()

csvfile = open(TAGS_SOURCE_FILE, 'r', newline='\n', encoding='utf8')
data = csv.reader(csvfile)
outcome = []
next(data)
for row in data:
    i = 0
    current_document_to_insert = {}
    for field in tags_fields:
            try:
                current_document_to_insert[field] = int(row[i])
            except ValueError:
                current_document_to_insert[field] = row[i]
            i += 1
    outcome.append(current_document_to_insert)

try:
    MOVIES_DB.tags.insert_many(outcome)
except BulkWriteError as bwe:
        print(bwe.details)
        raise

