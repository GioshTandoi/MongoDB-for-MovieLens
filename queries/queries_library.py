import re
import sys
from pprint import pprint

from pymongo.errors import OperationFailure
from constants import userId, movieId, genres, rating_value, MOVIES_DB, tag, tags_level_operations_file, \
    user_level_operations_file, ID_Field, overallRating, occurrences, total_count, genre, max_count
from functools import reduce

""" In this script there are different functions that can be used to perform queries on the database."""

""" Query 1: The purpose is to calculate the average rating of the movies that are associated with a given tag.
 So, the average for a specific tag. The tag might be one or many tags, that match with a specific regular expression.
"""


def find_average_rating_per_tag(db, tag_to_query):
    pattern = re.compile(tag_to_query, re.IGNORECASE)
    movie_ids_with_this_tag = list(MOVIES_DB.tags.distinct(movieId, {tag: pattern}))
    pipeline = [{"$match": {movieId: {"$in": movie_ids_with_this_tag}}},
                {"$group": {ID_Field: None, overallRating: {"$avg": "$rating"}}}]

    try:
        result = list(db.ratings.aggregate(pipeline, cursor={}))
    except OperationFailure:
        print("Something went Wrong", file=tags_level_operations_file)
        print(OperationFailure.details, file=tags_level_operations_file)
        sys.exit(1)
    return round(result[0][overallRating], 2)


""" Query 2: Given a tag, this query retrieves the number of occurrences of ratings for each value of rating. For 
example, suppose a given tag X is linked with N movies, each of these movies has different ratings from different users.
The purpose is to count the number of ratings for each value of rating that each of these
movies have"""


def count_different_rating_values_per_tag(db, tag_to_query):
    pattern = re.compile(tag_to_query, re.IGNORECASE)
    movie_ids_with_this_tag = list(MOVIES_DB.tags.distinct(movieId, {tag: pattern}))
    pipeline = [{"$match": {movieId: {"$in": movie_ids_with_this_tag}}},
                {"$group": {ID_Field: "$rating", occurrences: {"$sum": 1}}}
                ]
    try:
        result = list(db.ratings.aggregate(pipeline, cursor={}))
    except OperationFailure:
        print("Something went Wrong", file=tags_level_operations_file)
        print(OperationFailure.details, file=tags_level_operations_file)
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
        print("Something went Wrong", file=tags_level_operations_file)
        print(OperationFailure.details, file=tags_level_operations_file)
        sys.exit(1)
    return result[0][total_count]


""" 
Query 4: 
Given a userId, find all the genres of movies he rated and the average rating per each genre of movie. 
"""


def find_user_preferences(db, ID):
    ratings_by_user = list(db.ratings.find({userId: ID}))
    movie_ids_rated_by_user = list(db.ratings.distinct(movieId, {userId: ID}))

    pipeline = [
        {"$match": {"_id": {"$in": movie_ids_rated_by_user}}},
        {"$unwind": "$genres"},
        {"$group": {"_id": "$genres", "movies": {"$addToSet": "$_id"}}}
        ]

    try:
        aggregate_genre = list(db.movies.aggregate(pipeline, cursor={}))
    except OperationFailure:
        print("Something went Wrong", file=user_level_operations_file)
        print(OperationFailure.details, file=user_level_operations_file)
        sys.exit(1)
    preferences = []
    for genre in aggregate_genre:
        genre = substitute_movie_ids_with_rating_values(genre, ratings_by_user)
        avg = reduce(lambda x, y: x + y, genre['movies']) / len(genre['movies'])
        preferences.append({"userId": ID, "genre": genre['_id'], "overallRating": round(avg, 2)})
    return preferences


"""The pipeline aggregation stages in the find_user_preferences() function (line 88) retrieves a list of movieIds
as the value of the field "movies". However, what we really want, is a list of rating-values associated with each movie
genre the user rated, so that we can compute the average rating for each movie genre retrieved from the aggregation
stages. So, this helper function execute such substitution, swapping the MovieIds with their respective rating_value,
from this specific user."""


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
        print("Something went wrong with query -find_top_3_best_genres_for_user-", file=user_level_operations_file)
        print(OperationFailure.details, file=user_level_operations_file)
        sys.exit(1)

    return result


"""QUERY 6: Find all the movies that have been rated with the maximum number of 5 stars rating-value.
To compute such query, we need to pass through different steps. First, we need to calculate the maximum number
of times that a movie has been rated 5 stars. This is the what the find_max_five_stars_counter() function
retrieves. Then, we have to find, in the ratings collection, which are the movies that have such number 
as their counter_5_stars_rating value. This is what the find_best_movies_ids() retrieves. 
Finally, once we have all the movies ids with such maximum value, we can retrieve all their details from 
the movies collection (find_best_movies_details())."""


def find_max_five_stars_counter(db):
    pipeline = [{"$match": {rating_value: {"$eq": 5.0}}},
                {"$group": {ID_Field: "$movieId", total_count: {"$sum": 1}}},
                {"$group": {ID_Field: None, max_count: {"$max": "$total_count"}}}]
    try:
        result = list(db.ratings.aggregate(pipeline))
    except OperationFailure:
        print("Something went wrong with query -find_top_3_best_genres_for_user-", file=user_level_operations_file)
        print(OperationFailure.details, file=user_level_operations_file)
        sys.exit(1)
    return result[0][max_count]


def find_best_movies_ids(db):
    max_five_stars_counter = find_max_five_stars_counter(db)
    pipeline = [{"$match": {rating_value: {"$eq": 5.0}}},
                {"$group": {ID_Field: "$movieId", total_count: {"$sum": 1}}},
                {"$match": {total_count: {"$eq": max_five_stars_counter}}},
                {"$project": {ID_Field: 1}}]
    try:
        result = list(db.ratings.aggregate(pipeline))
    except OperationFailure:
        print("Something went wrong with query -find_top_3_best_genres_for_user-", file=user_level_operations_file)
        print(OperationFailure.details, file=user_level_operations_file)
        sys.exit(1)
    return list(map(lambda x: x[ID_Field], result))


def find_best_movies_details(db):
    movies_ids = find_best_movies_ids(db)
    return list(db.movies.find({ID_Field: {"$in": movies_ids}}))


"""
Query : 
Given a specific link-id, find all the movies in the movie-lens set, that are related to it, 
and retrieve the average rating for each of them.
imdbId_to_find = 113228
dbref = WIDER_MOVIES_DB.links.find_one({imdbId: imdbId_to_find})
print(dbref)
result = WIDER_MOVIES_DB.dereference(dbref[movieLensId])
print(result)
***********************************************************************************************************
This query is not executable, I wanted to compute such query but unfortunately PyMongo does NOT support 
DbRef dereference over two different databases. So the MOVIES_DB is not actually reachable through such
DbRefs. 
"""
