import sys
from __builtin__ import reduce

__author__ = 'Giorgia'

from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError, ConnectionFailure, ServerSelectionTimeoutError, OperationFailure
import csv
from bson.dbref import DBRef
from pymongo import WriteConcern
from batch_operations import movieId



def updateMovies_calculatedFields_tags(idS, moviesDatabase):
    for id  in idS:
        ratings_of_this_movie=moviesDatabase.ratings.find({movieId: id});
        for rating in ratings_of_this_movie:
            ratings_values=ratings_of_this_movie["rating"];
        if ratings_values!=0:
            avg_rating=reduce(lambda x, y: x + y, ratings_values) / len(ratings_values);

        tags_of_this_movie=moviesDatabase.tags.find({movieId});


