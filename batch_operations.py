import sys

__author__ = 'Giorgia'

from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError, ConnectionFailure, ServerSelectionTimeoutError, OperationFailure
import csv
from bson.dbref import DBRef
from pymongo import WriteConcern

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
movie_field_tags = 'tags'
ratings_fields = [userId, movieId, rating_value, timestamp]
links_fields = [movieId, imdbId, tmdbId]
tags_fields = [userId, movieId, tag, timestamp]

RATINGS_SOURCE_FILE = 'data/ratings.csv'
MOVIES_SOURCE_FILE = 'data/movies.csv'
TAGS_SOURCE_FILE = 'data/tags.csv'
LINKS_SOURCE_FILE = 'data/links.csv'

movies_collection = "movies"
movies_database = "moviesDB"

"""----------------------------------------- CONNECTING TO MONGODB---------------------------------------------
MongoClient() retrieves the mongodb instance of the running server, which, in this case, is the default one, 
running on the localhost on port 27017

client.moviesDB retrieves a reference to a mongodb database instance (moviesDB). If the database doesn't exists it
will be created. Note that Mongodb creates instances lazily, which means that the database instance will be created
only when effectively used. This is how Mongodb works with collections as well."""

client = MongoClient()
MOVIES_DB = client.moviesDB

"""
-------------------------------------------LOADING RATINGS COLLECTION-------------------------------------------
"""
try:
    MOVIES_DB.ratings.drop()  # this command drops the collection if exists, so that we can run this script any time.
# Without this command mongodb would reject the insertion and the script would crash,
# since it doesn't accept documents with an already existing ID.
except ServerSelectionTimeoutError:
    print("Unable to establish a connection.")
    print(ServerSelectionTimeoutError.args)
    sys.exit(1)


with open(RATINGS_SOURCE_FILE, newline='\n', encoding='utf8') as csv_file:
    data = csv.reader(csv_file)
    outcome = []
    next(data)  # this command removes the header of the csv file
    """Now we can loop over the just read file. Since all the values in each line of the file are 
     read as string values, some cast operations are needed"""
    for row in data:
        i = 0
        current_document_to_insert = {}
        for field in ratings_fields:
            if field == rating_value:
                try:
                    current_document_to_insert[field] = float(row[i])  # this cast operation is necessary
                    # since we need to compute calculations with rating values
                except ValueError:
                    current_document_to_insert[field] = row[i]
            else:
                try:
                    current_document_to_insert[field] = int(row[i])  # this line casts the input fields to a numeric
                    # type if an int value is recognised. Substituting string values with numeric types makes operating
                    # on the DB more efficient. For example indexing the ID field
                except ValueError:
                    current_document_to_insert[field] = row[i]  # if the value is not a numeric one, than it is added as
                    # it is
            i += 1

        outcome.append(current_document_to_insert)


"""Now that we filled the outcome variable with a list of json-like objects, we can add the list to the ratings 
collection. This works only when the dataset to import is relatively small, just like in this case. That's because
'outcome' is a run-time variable an so it is loaded in the main memory. This means that if the csv file that we are 
reading from is muc bigger it may lead to an OutOfMemoryError and the script would crash. In such case we can 
use the 'insert_one()' method instead, directly in the previous outer loop, once the 'current_document_to_insert'
is ready. Note that PyMongo automatically split the batch into smaller sub-batches based
on the maximum message size accepted by MongoDB, supporting very large bulk insert operations."""

try:
    MOVIES_DB.ratings.insert_many(outcome)
except BulkWriteError as bwe:
        print(bwe.details)
        raise

"""-------------------------------------------LOADING MOVIES COLLECTION----------------------------------------------"""
"""
The two lines below, execute a grouping query to the ratings collection, so that the calculated 
values can then be added as fields of the movies collection. 
Mongodb 3.4 deprecates the db.collection.group() method. The method db.collection.aggregate() 
with the $group stage or db.collection.mapReduce() should be used instead.
"""

pipeline = [{"$group": {"_id": "$movieId", "averageRating": {"$avg": "$rating"}, "count": {"$sum": 1}}}]
try:
    cursor = MOVIES_DB.ratings.aggregate(pipeline, cursor={})
except OperationFailure:
    print("Unable to establish a connection.")
    print(OperationFailure.details)
    sys.exit(1)

"""The method 'aggregate' retrieves a cursor object. We can loop over cursors only once, but since we need
to loop over the resulted list for every document we add to the movies collection, this for loop
creates a list object with the items in the cursor, so that we can loop over the items every time we want to."""

group_result_list = []
for doc in cursor:
    group_result_list.append(doc)

try:
    MOVIES_DB.movies.drop()
except ServerSelectionTimeoutError:
    print("Unable to establish a connection.")
    print(ServerSelectionTimeoutError.args)
    sys.exit(1)


"""This for loop has been wrapped into a function so that we don't have to add an other for in the already two 
 nested for loops int the lines below. """


def find(list_of_documents, identifier):
    for d in list_of_documents:
        if d[ID_Field] == identifier:
            return d
    return 0


with open(MOVIES_SOURCE_FILE, newline='\n', encoding='utf8') as csv_file:
    data = csv.reader(csv_file)
    outcome = []
    next(data)
    for row in data:
        i = 0
        current_document_to_insert = {}
        for field in movies_fields:
            if field == genres:
                current_document_to_insert[field] = row[i].split('|')
            else:
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


"""As mentioned before, this only works when the file we are reading from is relatively small. """
try:
    MOVIES_DB.movies.insert_many(outcome)
except BulkWriteError as bwe:
        print(bwe.details)
        raise


"""-------------------------------------- LOADING TAGS COLLECTION----------------------------------------------------"""
try:
    MOVIES_DB.tags.drop()
except ServerSelectionTimeoutError:
    print("Unable to establish a connection.")
    print(ServerSelectionTimeoutError.args)
    exit(1)

with open(TAGS_SOURCE_FILE, newline='\n', encoding='utf8') as csv_file:
    data = csv.reader(csv_file)
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


"""-----------------------------------------------UPDATING MOVIES WITH NEW TAGS---------------------------------"""


def remove_movie_id_field(tag_document_list):
    for tag_document in tag_document_list:
        del tag_document[movieId]


print(MOVIES_DB.movies.find())
for movie in list(MOVIES_DB.movies.find()):
    tags = list(MOVIES_DB.tags.find({movieId: movie[ID_Field]}))
    if len(tags) != 0:
        remove_movie_id_field(tags)
        MOVIES_DB.movies.update({ID_Field: movie[ID_Field]}, {"$set": {movie_field_tags: tags}}, upsert=False)


""" ----------------------------------------------LOADING LINKS IN AN OTHER DATABASE-------------------------------"""

WIDER_MOVIES_DB = client.widerMoviesDb

try:
    WIDER_MOVIES_DB.links.drop()
except ServerSelectionTimeoutError:
    print(ServerSelectionTimeoutError.args)
    print("Unable to establish a connection")
    exit(1)

with open(LINKS_SOURCE_FILE, newline='\n', encoding='utf8') as csv_file:
    data = csv.reader(csv_file)
    outcome = []
    next(data)
    for row in data:
        i = 0
        current_document_to_insert = {}
        for field in links_fields:
            if field == movieId:
                current_document_to_insert[field] = DBRef(movies_collection, row[i], movies_database)
            else:
                try:
                    current_document_to_insert[field] = int(row[i])
                except ValueError:
                    current_document_to_insert[field] = row[i]
            i += 1

        outcome.append(current_document_to_insert)


try:
    WIDER_MOVIES_DB.links.insert_many(outcome)
except BulkWriteError as bwe:
        print(bwe.details)
        raise

"""-------------------------------------------DATA ACCESS ON MOVIES COLLECTION-----------------------------"""

# CREATING THE INDEX

MOVIES_DB.movies.create_index(movie_field_tags)
tag_inner_field = 'tag'

first_query = list(MOVIES_DB.movies.find({movie_field_tags: {'$elemMatch': {tag_inner_field: "love"}}}))

for row_result in first_query:
    print(row_result)


"""-----------------------------------More examples on CRUD operations ----------------------------------------------"""
""" PyMongo also supports executing mixed bulk write operations. A batch of insert, update, and remove 
operations can be executed together through bulk_write() method.
"""

movies = MOVIES_DB.get_collection(movies_collection, write_concern=WriteConcern(w=3, wtimeout=1))

try:
    movies.bulk_write([InsertOne({'a': i}) for i in range(4)])
except BulkWriteError as bwe:
    print(bwe.details)

"""
...
{'nInserted': 4,
'nMatched': 0,
'nModified': 0,
'nRemoved': 0,
'nUpserted': 0,
'upserted': [],
'writeConcernErrors': [{u'code': 64...
                       u'errInfo': {u'wtimeout': True},
                       u'errmsg': u'waiting for replication timed out'}],
'writeErrors': []}"""