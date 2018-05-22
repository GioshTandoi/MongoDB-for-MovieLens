__author__ = 'Giorgia'
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError, WriteError
import csv
from bson.dbref import DBRef
from constants import *

"""
-------------------------------------------LOADING RATINGS COLLECTION-------------------------------------------
"""


def load_ratings_from_csv(source_file, collection):
    with open(source_file, newline='\n', encoding='utf8') as csv_file:
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
                        # type if an int value is recognised. Substituting string values with numeric types makes
                        # operating on the DB more efficient. For example indexing the ID field
                    except ValueError:
                        current_document_to_insert[field] = row[i]  # if the value is not a numeric one, than it
                        # is added as it is
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
        collection.insert_many(outcome, ordered=False)  # order parameter set to false ensures that
        # all document inserts will be attempted
        return True
    except BulkWriteError:
            return False


"""-------------------------------------------LOADING MOVIES COLLECTION----------------------------------------------"""


def load_movies_from_csv(source_file, collection):
    with open(source_file, newline='\n', encoding='utf8') as csv_file:
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
            outcome.append(current_document_to_insert)

    """As mentioned before, this only works when the file we are reading from is relatively small. """
    try:
        collection.insert_many(outcome, ordered=False)
        return True
    except BulkWriteError:
        return False


"""-------------------------------------- LOADING TAGS COLLECTION----------------------------------------------------"""


def load_tags_from_csv(source_file, collection):

    with open(source_file, newline='\n', encoding='utf8') as csv_file:
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
        collection.insert_many(outcome)
        return True
    except BulkWriteError:
        return False


"""-----------------------------------------------UPDATING MOVIES WITH NEW TAGS---------------------------------"""


def remove_movie_id_field(tag_document_list):
    for tag_document in tag_document_list:
        del tag_document[movieId]


def update_movies_add_tags(movies_collection, tags_collection):
    for movie in list(movies_collection.find()):
        tags = list(tags_collection.find({movieId: movie[ID_Field]}))
        if len(tags) != 0:
            remove_movie_id_field(tags)
            try:
                movies_collection.update({ID_Field: movie[ID_Field]}, {"$set": {movies_field_tags: tags}}, upsert=False)
            except WriteError:
                return False
    return True


""""-------------------------------------UPDATING MOVIES WITH NEW CALCULATED FIELDS--------------------------------"""

"""This for loop has been wrapped into a function so that we don't have to add an other for in the already two 
 nested for loops int the lines below. """


def find_in_group_result(list_of_documents, identifier):
    for d in list_of_documents:
        if d[ID_Field] == identifier:
            return d
    return 0


def update_movies_add_calculated(movies_collection, group_result_from_ratings):
    for movie in list(movies_collection.find()):
        calculated_fields = find_in_group_result(group_result_from_ratings, movie[ID_Field])
        if calculated_fields != 0:
            try:
                movies_collection.bulk_write([
                    UpdateOne({ID_Field: movie[ID_Field]},
                              {'$set': {movies_field_avg: round(calculated_fields['averageRating'], 2)}},
                              upsert=False),
                    UpdateOne({ID_Field: movie[ID_Field]}, {'$set': {movies_field_count: calculated_fields['count']}},
                              upsert=False)])
            except WriteError:
                return False
    return True


""" ----------------------------------------------LOADING LINKS IN AN OTHER DATABASE-------------------------------"""


def load_links_from_csv(source_file, collection, movies_collection, movies_database):
    with open(source_file, newline='\n', encoding='utf8') as csv_file:
        data = csv.reader(csv_file)
        outcome = []
        next(data)
        for row in data:
            i = 0
            current_document_to_insert = {}
            for field in links_fields:
                if field == movieLensId:
                    current_document_to_insert[field] = DBRef(moviesLens_collection_name, row[i], moviesLens_db_name)
                else:
                    try:
                        current_document_to_insert[field] = int(row[i])
                    except ValueError:
                        current_document_to_insert[field] = row[i]
                i += 1

            outcome.append(current_document_to_insert)

    try:
        collection.insert_many(outcome, ordered=False)
        return True
    except BulkWriteError:
        return False
