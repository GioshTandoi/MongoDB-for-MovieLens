from pymongo import MongoClient

client = MongoClient()

db = client.movies

collection = db.movies

collection.insert_one({'giorgia' : ' giorgia'})
