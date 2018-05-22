from pymongo import UpdateOne
from pymongo.errors import WriteError

__author__ = 'Giorgia'
from constants import *
from functools import reduce


"""These are some functions that can be called when inserting operations happen on-line.
Since movies documents contain fields that depend on the content of the tag and rating collections,
whenever another rating is added the corresponding movie document needs to be updated, the same
for tags. Here we simulate the insertion of a new rating document and of a new tag document,
coming, for example, from a user that is using the application on-line, and then the update operations
that has to be executed (even if not in a real transaction, that is: they can happen asynchronously).
We can run this after the batch operations have populated the DB"""


def update_one_movie_calculated_fields(id_of_this_movie, database):
    ratings_of_this_movie = database.ratings.find({movieId: id_of_this_movie})
    ratings_values_for_this_movie = []
    for rating in ratings_of_this_movie:
        ratings_values_for_this_movie.append(rating[rating_value])
    if len(ratings_values_for_this_movie) != 0:
        avg_rating = reduce(lambda x, y: x + y, ratings_values_for_this_movie) / len(ratings_values_for_this_movie)
        try:
            result = database.movies.bulk_write([
                UpdateOne({ID_Field: id_of_this_movie},
                          {'$set': {movies_field_avg: round(avg_rating, 2)}},
                          upsert=False),
                UpdateOne({ID_Field: id_of_this_movie}, {'$set': {movies_field_count: len(rating_value)}},
                          upsert=False), ])
            return result.bulk_api_result
        except WriteError:
            return False


def remove_movie_id_field(tag_document_list):
    for tag_document in tag_document_list:
        del tag_document[movieId]


def add_tags_for_this_movie(id_of_this_movie, database):
    tags = list(database.tags.find({movieId: id_of_this_movie}))
    if len(tags) != 0:
        remove_movie_id_field(tags)
        try:
            database.movies.update({movieId: id_of_this_movie}, {"$set": {movies_field_tags: tags}}, upsert=False)
            return True
        except WriteError:
            return False


movie_to_rate = 160567
MOVIES_DB.ratings.insert_one({userId: "giorgia", movieId: movie_to_rate, rating_value: 4, timestamp: 1019127444})
update_one_movie_calculated_fields(movie_to_rate, MOVIES_DB)

MOVIES_DB.tags.insert_one({userId:"giorgia", movieId: movie_to_rate, tag: "boring", timestamp: 1019127444})
add_tags_for_this_movie(movie_to_rate, MOVIES_DB)