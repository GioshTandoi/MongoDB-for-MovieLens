"""This script performs some operations with the queries_library.py functions
"""
from pprint import pprint
from pprint import PrettyPrinter
from pymongo.errors import BulkWriteError
import constants

from queries.queries_library import *
"""--------------------------------FIND PREFERENCES OF USER N.1 ------------------------------------------"""

pp = PrettyPrinter(indent=4, stream=constants.user_level_operations_file)
user = 1
first_query_result = find_user_preferences(MOVIES_DB, user)
print("These are the preferences of user n. 1", file=constants.user_level_operations_file)
pp.pprint(first_query_result)
"""--------------------------------CREATING USER PREFERENCES COLLECTION-----------------------------------"""
all_users = list(constants.MOVIES_DB.ratings.distinct(userId))

for user in all_users:
    try:
        MOVIES_DB.users_preferences.insert_many(find_user_preferences(constants.MOVIES_DB, user))
    except BulkWriteError:
        print("Something went wrong when inserting User preferences", file=constants.user_level_operations_file)
        print(BulkWriteError.details)
        sys.exit(1)
print("\n",file=constants.user_level_operations_file)
print("*******************",file=constants.user_level_operations_file)
print("User-preferences collection loaded correctly", file=constants.user_level_operations_file)
print("\n",file=constants.user_level_operations_file)

"""*******************************  QUERY ON USER_PREFERENCES********************************************"""
user = 1
top_3_genres_for_user = find_top_3_best_genres_for_user(constants.MOVIES_DB, user)
print("These are the top 3 genres for user n. 1", file=constants.user_level_operations_file)
pp.pprint(top_3_genres_for_user)

"""--------------------------------QUERIES ON TAGS---------------------------------------------------------"""
pp = PrettyPrinter(indent=4, stream=constants.tags_level_operations_file)
tag_to_query = "love"

"""FIRST QUERY"""
first_query_on_tags = find_average_rating_per_tag(constants.MOVIES_DB,tag_to_query)
print("This is the average rating of the movies that are related with the tag " + tag_to_query,
      file=constants.tags_level_operations_file)
pp.pprint(first_query_on_tags)

"""SECOND QUERY"""
second_query_on_tags = count_different_rating_values_per_tag(constants.MOVIES_DB,tag_to_query)
print("These are different ratings values related to the tag " + tag_to_query +", with all their occurrences",
      file=constants.tags_level_operations_file)
pp.pprint(second_query_on_tags)

"""THIRD QUERY"""
third_query_on_tags = find_number_of_ratings_related_to_tag(constants.MOVIES_DB, tag_to_query)
print("This is the number of ratings related to the tag " + tag_to_query,
      file=constants.tags_level_operations_file)
pp.pprint(third_query_on_tags)
