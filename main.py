__author__ = 'Giorgia'

from pymongo import MongoClient
from pymongo.errors import BulkWriteError, ConnectionFailure
import csv

"""-----------------------------------------------------CONSTANTS DECLARATION-------------------------- """
movieId = 'movieId'
title = 'title'
genres = 'genres'

userId = 'userId'
rating = 'rating'
timestamp = 'timestamp'

imdbId = 'imdbId'
tmdbId = 'tmdbId'

tag = 'tag'

movies_fields = ['_id', title, genres]
ratings_fields = [userId, movieId, rating, timestamp]
links_fields = [movieId, imdbId, tmdbId]
tags_fields = [userId, movieId, tag, timestamp]

"""This function updates a collection adding new entries or creates it if it doesn't exists.
 It takes three parameters: collection which is the collection object to which we want to add entries; 
 input_csv which is a csv file containing the entries to add, this file is converted to a dictionary python type 
 which can be stored in mongodb as a sequence of documents belonging to the same collection; last but not least, 
 the fields parameter is a sequence of parameters that represent the JSON fields of the specific collection. 



def import_collection(collection, input_csv, fields):

    with open(input_csv, newline='\n', encoding='utf8') as csvfile:
        data = csv.reader(csvfile)
        output = []

        for row in data:
            i = 0
            result = {}
            for field in fields:
                result[field] = row[i]
                i += 1
            output.append(result)

    try:
        just_inserted = collection.insert_many(output)

    except BulkWriteError as bwe:
        print(bwe.details)
        raise

    return just_inserted

 """
"""----------------------------------------- CONNECTING TO MONGODB---------------------------------------------
MongoClient() retrieves the mongodb instance of the running server, which, in this case, is the default one, 
running on the localhost on port 27017

client.moviesDB retrieves a reference to a mongodb database instance (moviesDB). If the database doesn't exists it
will be created"""

client = MongoClient()
db = client.moviesDB

collection = db.movies
collection.drop()

csvfile = open('data/movies.csv', 'r', newline='\n', encoding='utf8')
data = csv.reader(csvfile)


for row in data:
    i = 0
    result = {}
    for field in movies_fields:
        result[field] = row[i]
        i += 1
    try:
        collection.insert_one(result)
    except BaseException as bwe:
        print(bwe.details)


collection = db.ratings
collection.drop()
csvfile = open('data/ratings.csv', 'r', newline='\n', encoding='utf8')
data = csv.reader(csvfile)
outcome = []

for row in data:
    i = 0
    result = {}
    for field in ratings_fields:
        result[field] = row[i]
        i += 1
    outcome.append(result)

inserted = collection.insert_many(outcome)
inserted.inserted_ids


