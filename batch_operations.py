"""This script contains operations to populate the db as it first starts.
They are intended to run as batch operations"""
import sys
from pymongo import WriteConcern, ReadPreference
from pymongo.errors import OperationFailure
from pymongo.read_concern import ReadConcern
import constants
from batch_operations_library import *

"""---------------------------------------CREATING COLLECTIONS----------------------------------------------"""
"""Creating collections in this way allows to change the values of read_preference, read_concern and write_concern, 
and to calibrate them according to our needs. In this case we do not have a replica set and the parameters have been 
set accordingly."""

movies = MOVIES_DB.get_collection(movies_collection_name, read_preference=ReadPreference.PRIMARY,
                                  write_concern=WriteConcern(w=1, wtimeout=1),
                                  read_concern=ReadConcern("local"))
ratings= MOVIES_DB.get_collection(ratings_collection_name, read_preference=ReadPreference.PRIMARY,
                                  write_concern=WriteConcern(w=1, wtimeout=1),
                                  read_concern=ReadConcern("local"))
tags=MOVIES_DB.get_collection(tags_collection_name, read_preference=ReadPreference.PRIMARY,
                              write_concern=WriteConcern(w=1, wtimeout=1),
                              read_concern=ReadConcern("local"))
links=MOVIES_DB.get_collection(links_collection_name)

"""-----------------------------------LOADING DATA TO MOVIES_DB--------------------------------------------"""

check_movies = load_movies_from_csv(MOVIES_SOURCE_FILE, movies)
check_tags = load_tags_from_csv(TAGS_SOURCE_FILE, tags)
check_ratings = load_ratings_from_csv(RATINGS_SOURCE_FILE, ratings)
if check_movies:
    print("LOADING MOVIES FROM SIMPLE CSV FILE: SUCCESS", file=open("operations_log.txt", "a"))
else:
    print("LOADING MOVIES FAILED", file=open("operations_log.txt", "a"))
    sys.exit(1)

if check_ratings:
    print("LOADING RATINGS FROM CSV FILE: SUCCESS", file=open("operations_log.txt", "a"))
else:
    print("LOADING RATINGS FAILED", file=open("operations_log.txt", "a"))
    sys.exit(1)

if check_tags:
    print("LOADING TAGS FROM CSV FILE: SUCCESS", file=open("operations_log.txt", "a"))
else:
    print("LOADING TAGS FAILED", file=open("operations_log.txt", "a"))
    sys.exit(1)


"""----------------------------------------------FIRST UPDATE TO MOVIES----------------------------------------"""
"""

The lines below, execute a grouping query to the ratings collection, so that the calculated
values can then be added as fields of the movies collection.
Mongodb 3.4 deprecates the db.collection.group() method. The method db.collection.aggregate()
with the $group stage or db.collection.mapReduce() should be used instead.
"""

pipeline = [{"$group": {"_id": "$movieId", "averageRating": {"$avg": "$rating"}, "count": {"$sum": 1}}}]
try:
    cursor = ratings.aggregate(pipeline, cursor={})
except OperationFailure:
    print("Something went Wrong", file=open("operations_log.txt", "a"))
    print(OperationFailure.details, file=open("operations_log.txt", "a"))
    sys.exit(1)

"""The method 'aggregate' retrieves a cursor object. We can loop over cursors only once, but since we need
to loop over the resulted list for every document we add to the movies collection, this for loop
creates a list object with the items in the cursor, so that we can loop over the items every time we want to."""

group_result_list = []
for doc in cursor:
    group_result_list.append(doc)


result = update_movies_add_calculated(movies, group_result_list)
print("UPDATING MOVIES: ADDING CALCULATED FIELDS:", file=open("operations_log.txt", "a"))
print(result, file=open("operations_log.txt", "a"))


"""------------------------------------------SECOND UPDATE TO MOVIES-----------------------------------------"""
result = update_movies_add_tags(movies, tags)
print("UPDATING MOVIES: ADDING TAGS:", file=open("operations_log.txt", "a"))
print(result, file=open("operations_log.txt", "a"))


"""---------------------------------------------VERIFYING DATABASE CONTENT-------------------------------------"""
"""The result has been limited to 10, so that files don't become too heavy"""
print("MOVIES COLLECTION", file=open("results/movies.txt", "a"))
for doc in movies.find().limit(10):
    try:
        print(doc, file=open("results/movies.txt", "a"))
    except UnicodeEncodeError:
        break

print("RATINGS COLLECTION", file=open("results/ratings.txt", "a"))
for doc in ratings.find().limit(10):
    try:
        print(doc, file=open("results/ratings.txt", "a"))
    except UnicodeEncodeError:
        break

print("TAGS COLLECTION", file=open("results/tags.txt", "a"))
for doc in tags.find().limit(10):
    try:
        print(doc, file=open("results/tags.txt", "a"))
    except UnicodeEncodeError:
        break

"""-------------------------------------------DATA ACCESS ON MOVIES COLLECTION-----------------------------"""

# CREATING THE INDEX

movies.create_index(constants.movies_field_tags)

first_query = list(MOVIES_DB.movies.find({constants.movies_field_tags:{'$elemMatch':{constants.movies_tag_inner_field: "love"}}}))

print("\n\nRESULT FROM FIRST QUERY TO MOVIES COLLECTION: \nAll documents with love as a tag\n",
      file=open("results/query_on_movies.txt", "a"))
for row_result in first_query:
    print(row_result, file=open("results/query_on_movies.txt", "a"))

"""----------------------------------------CREATING LINKS COLLECTION IN ANOTHER DATABASE-----------------------------"""

links = WIDER_MOVIES_DB.get_collection(links_collection_name, read_preference=ReadPreference.SECONDARY,
                                       write_concern=WriteConcern(w=1, wtimeout=1), read_concern=ReadConcern("available"))

if load_links_from_csv(LINKS_SOURCE_FILE, links, movies, MOVIES_DB):
    print("\n\nLOADING LINKS FROM CSV FILE: SUCCESS\n\n", file=open("operations_log.txt", "a"))
else:
    print("LOADING LINKS FROM CSV FAILED\n", file=open("operations_log.txt", "a"))

print("LINKS COLLECTION", file=open("results/links.txt", "a"))
for doc in links.find().limit(10):
    try:
        print(doc, file=open("results/links.txt", "a"))
    except UnicodeEncodeError:
        break
