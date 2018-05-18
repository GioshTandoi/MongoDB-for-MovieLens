__author__ = 'Giorgia'

from pymongo import MongoClient
from pymongo.errors import BulkWriteError, ConnectionFailure
import csv
import pprint

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
"""
def parse_int(string):
    return int(''.join([x for x in string if x.isdigit()])) """

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

collection = db.ratings
collection.drop()
csvfile = open('data/ratings.csv', 'r', newline='\n', encoding='utf8')
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

inserted = collection.insert_many(outcome)


collection = db.movies
collection.drop()

csvfile = open('data/movies.csv', 'r', newline='\n', encoding='utf8')
data = csv.reader(csvfile)
outcome = []

"""
Deprecated since version 3.4: Mongodb 3.4 deprecates the 
db.collection.group() method. Use db.collection.aggregate() 
with the $group stage or db.collection.mapReduce() instead.
"""


pipeline = [{"$group": {"_id": "$movieId", "averageRate": {"$avg": "$rating"}, "count": {"$sum": 1}}}]
cursor = db.ratings.aggregate(pipeline, cursor={})

group_result_list = []
for doc in cursor:
    group_result_list.append(doc)


def find(list_of_documents, identifier):
    for d in list_of_documents:
        if d['_id'] == identifier:
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
    calculated_fields = find(group_result_list, current_document_to_insert['_id'])
    if calculated_fields != 0:
        current_document_to_insert['averageRate'] = calculated_fields['averageRate']
        current_document_to_insert['count'] = calculated_fields['count']

    outcome.append(current_document_to_insert)

try:
    db.movies.insert_many(outcome)
except BaseException as bwe:
        print(bwe.details)


