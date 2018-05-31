import re
import sys
from pymongo.errors import OperationFailure
from constants import userId, movieId, genres, rating_value, MOVIES_DB, tag, tags_level_operations_file, \
    user_level_operations_file, ID_Field, overallRating, occurrences, total_count, genre
from functools import reduce

""" In this script there are different functions that can be used to perform queries on the database."""

""" Query 1: the purpose is to calculate the average rating of the films that are associated with a given tag.
 So average for a specific tag. The tag might be one or many tags that match with a specific regular expression
"""


def find_average_rating_per_tag(db, tag_to_query):
    pattern = re.compile(tag_to_query, re.IGNORECASE)
    movie_ids_with_this_tag = list(MOVIES_DB.tags.distinct(movieId, {tag: pattern}))
    pipeline = [{"$match": {movieId: {"$in": movie_ids_with_this_tag}}},
                {"$group": {ID_Field: None, overallRating: {"$avg": "$rating"}}}]

    try:
        result = list(db.ratings.aggregate(pipeline, cursor={}))
    except OperationFailure:
        print("Something went Wrong", tags_level_operations_file)
        print(OperationFailure.details, tags_level_operations_file)
        sys.exit(1)
    return round(result[0][overallRating], 2)


""" Query 2: Given a tag retrieve the number of occurrences of ratings for each value of rating. For example
suppose a given tag X is linked with N films, each of these film has different ratings from different users.
The purpose is to count the number of ratings for each value of rating that each of these
films have"""


def count_different_rating_values_per_tag(db, tag_to_query):
    pattern = re.compile(tag_to_query, re.IGNORECASE)
    movie_ids_with_this_tag = list(MOVIES_DB.tags.distinct(movieId, {tag: pattern}))
    pipeline = [{"$match": {movieId: {"$in": movie_ids_with_this_tag}}},
                {"$group": {ID_Field: "$rating", occurrences: {"$sum": 1}}}
                ]
    try:
        result = list(db.ratings.aggregate(pipeline, cursor={}))
    except OperationFailure:
        print("Something went Wrong", tags_level_operations_file)
        print(OperationFailure.details, tags_level_operations_file)
        sys.exit(1)
    return result


"""
Query 3: Find the number of ratings that a specified tag received.
"""


def find_number_of_ratings_related_to_tag(db, tag_to_query):
    pattern = re.compile(tag_to_query, re.IGNORECASE)
    pipeline = [{"$match": {"tags.tag": pattern}},
                {"$group": {ID_Field: None, total_count: {"$sum": "$numberOfRatings"}}}]
    try:
        result = list(db.movies.aggregate(pipeline, cursor={}))
    except OperationFailure:
        print("Something went Wrong", tags_level_operations_file)
        print(OperationFailure.details, tags_level_operations_file)
        sys.exit(1)
    return result[0][total_count]


""" 
Query 4: 
given a user, find all the genres of movies he rated and the average rating per each genre of movie. 
Then find other users wo rated at least onw movie wit a genre that is comprised in the genres the first user rated
and calculate ho similar they are, then select only those with a degree of similarity greater than 0.5.
the users that are similar to him, based on the genres of movies they rated. """


def find_user_preferences(db, ID):
    ratings_by_user = list(db.ratings.find({userId: ID}))
    movie_ids_rated_by_user = list(db.ratings.distinct(movieId, {userId: ID}))

    pipeline = [
        {"$match": {"_id": {"$in": movie_ids_rated_by_user}}},
        {"$unwind": "$genres"},
        {"$group": {"_id": "$genres", "movies": {"$addToSet": "$_id"}}}
        ]

    try:
        cursor = db.movies.aggregate(pipeline, cursor={})
    except OperationFailure:
        print("Something went Wrong", user_level_operations_file)
        print(OperationFailure.details, user_level_operations_file)
        sys.exit(1)

    aggregate_genre = []
    for c in cursor:
        aggregate_genre.append(c)

    preferences = []

    for genre in aggregate_genre:
        genre = substitute_movie_ids_with_rating_values(genre, ratings_by_user)
        avg = reduce(lambda x, y: x + y, genre['movies']) / len(genre['movies'])
        preferences.append({"userId": ID, "genre": genre['_id'], "overallRating": round(avg, 2)})
    return preferences


def substitute_movie_ids_with_rating_values(result_per_genre, ratings_by_user):
    substituted = []
    for movie in result_per_genre['movies']:
        for rating in ratings_by_user:
            if rating[movieId] == movie:
                substituted.append(rating[rating_value])

    result_per_genre['movies'] = substituted
    return result_per_genre


""""Query 5: given a user, find in the user_preferences collection, the best 3 genres for that user, 
based on the overallRating"""


def find_top_3_best_genres_for_user(db, ID):
    pipeline = [{"$match": {userId: ID}},
                {"$sort": {"overallRating": -1}},
                {"$limit": 3},
                {"$project": {ID_Field: 0, genre: 1, overallRating: 1}}]
    try:
        result = list(db.users_preferences.aggregate(pipeline))
    except OperationFailure:
        print("Something went wrong with query -find_top_3_best_genres_for_user-", user_level_operations_file)
        print(OperationFailure.details, user_level_operations_file)
        sys.exit(1)

    return result



"""
Query : 
Given a regular expression of  a specific link id, find all the movies in the movie-lens set, that are related to it, 
and retrieve the average rating for each of them
imdbId_to_find = 113228
dbref = WIDER_MOVIES_DB.links.find_one({imdbId: imdbId_to_find})
print(dbref)
result = WIDER_MOVIES_DB.dereference(dbref[movieLensId])
print(result)
"""