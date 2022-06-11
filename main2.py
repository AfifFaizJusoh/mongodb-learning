from dotenv import load_dotenv, find_dotenv
import os 
import pprint
from numpy import ndarray
printer = pprint.PrettyPrinter()
from pymongo import MongoClient
from datetime import datetime as dt
load_dotenv(find_dotenv())

# Get mongodb password from environment variable
password = os.environ.get('MONGODB_PWD')

# define connection string for mongodb - get from your mongodb database
connection_string = f'mongodb+srv://afif:{password}@tutorial.819uiqo.mongodb.net/?retryWrites=true&w=majority&authSource=admin'

# call client to connect to mongodb
client = MongoClient(connection_string)

dbs = client.list_database_names() # get list of all databases
production = client.production # create a new dbs / select a dbs

def create_book_collection():
    book_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["title", "authors", "publish_date", "type", "copies"],
            "properties": {
                "title": {
                    "bsonType": "string",
                    "description": "must be a string and is required"
                },
                "authors": {
                    "bsonType": "array",
                    "items": {
                        "bsonType": "objectId",
                        "description": "must be an array of objectIds"
                    }
                },
                "publish_date": {
                    "bsonType": "date",
                    "description": "must be a date and is required"
                },
                "type": {
                    "enum": ["Fiction", "Non-Fiction"],
                    "description": "must be Fiction or Non-Fiction"
                },
                "copies": {
                    "bsonType": "int",
                    "minimum": 0,
                    "description": "must be an integer and is required"
                },
            }
        }
    }

    try:
        production.create_collection("book")
    except Exception as e:
        print(e)
        
    production.command("collMod", "book", validator=book_validator)
    

def create_author_collection():
    author_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["first_name", "last_name", "date_of_birth"],
            "properties": {
                "first_name": {
                    "bsonType": "string",
                    "description": "must be a string and is required"
                },
                "last_name": {
                    "bsonType": "string",
                    "description": "must be a string and is required"
                },
                "date_of_birth": {
                    "bsonType": "date",
                    "description": "must be a date and is required"
                },
            }
        }
    }
    
    try:
        production.create_collection("author")
    except Exception as e:
        print(e)
        
    production.command("collMod", "author", validator=author_validator)
    
    
def create_data():
    authors = [
        {
            "first_name": "Afif",
            "last_name": "Faiz",
            "date_of_birth": dt(1993, 2, 27)
        },
        {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": dt(1980, 3, 1)
        },
        {
            "first_name": "Jane",
            "last_name": "Doe",
            "date_of_birth": dt(1989, 4, 1)
        },
        {
            "first_name": "Mary",
            "last_name": "Doe",
            "date_of_birth": dt(1988, 11, 13)
        }
    ]
    author_collection = production.author
    authors = author_collection.insert_many(authors).inserted_ids
    
    books = [
        {
            "title": "The Lord of the Rings",
            "authors": [authors[0]],
            "publish_date": dt(2000, 9, 29),
            "type": "Fiction",
            "copies": 1
        },
        {
            "title": "The Hobbit",
            "authors": [authors[1]],
            "publish_date": dt(1937, 9, 21),
            "type": "Fiction",
            "copies": 1
        },
        {
            "title": "The Catcher in the Rye",
            "authors": [authors[2]],
            "publish_date": dt(1951, 8, 21),
            "type": "Fiction",
            "copies": 1
        },
        {
            "title": "The Great Gatsby",
            "authors": [authors[3]],
            "publish_date": dt(1925, 4, 10),
            "type": "Fiction",
            "copies": 1
        },
        {
            "title": "The Da Vinci Code",
            "authors": [authors[0]],
            "publish_date": dt(2003, 3, 3),
            "type": "Non-Fiction",
            "copies": 1
        }
        
    ]
    book_collection = production.book
    book_collection.insert_many(books)
    

books_containing_a = production.book.find({"title": {"$regex": "a{1}"}})
# printer.pprint(list(books_containing_a))

# authors_and_book = production.author.aggregate([{
#    "$lookup": {
#        "from": "book",
#        "localField": "_id",
#        "foreignField": "authors",
#        "as": "books"
#    }
# }])

# printer.pprint(list(authors_and_book))


# authors_book_count = production.author.aggregate([
#     {
#         "$lookup": {
#             "from": "book",
#             "localField": "_id",
#             "foreignField": "authors",
#             "as": "books"
#         }
#     },
#     {
#         "$addFields": {
#             "total_books": {"$size": "$books"}
#         }
#     },
#     {
#         "$project": {"_id": 0, "first_name": 1, "last_name": 1, "total_books": 1}
#     }
# ])

# printer.pprint(list(authors_book_count))


# books_with_old_authors = production.book.aggregate([
#     {
#         "$lookup": {
#             "from": "author",
#             "localField": "authors",
#             "foreignField": "_id",
#             "as": "authors"
#         }
#     },
#     {
#         "$set": {
#             "authors": {
#                 "$map": {
#                     "input": "$authors",
#                     "in": {
#                         "age": {
#                             "$dateDiff": {
#                                 "startDate" : "$$this.date_of_birth",
#                                 "endDate": "$$NOW",
#                                 "unit": "year"
#                             }
#                         },
#                         "first_name": "$$this.first_name",
#                         "last_name": "$$this.last_name"
#                     }
#                 }
#             }
#         }
#     },
#     {
#         "$match": {
#             "$and": [
#                 {"authors.age": {"$gte": 30}},
#                 {"authors.age": {"$lte": 40}}
#             ]
#         }
#     },
#     {
#         "$sort": {
#             "age": 1
#         }
#     }
# ])
# printer.pprint(list(books_with_old_authors))

import pyarrow
from pymongoarrow.api import Schema 
from pymongoarrow.monkey import patch_all 
import pymongoarrow as pma
from bson import ObjectId

patch_all()

author = Schema({"_id": ObjectId, "first_name": pyarrow.string(), "last_name": pyarrow.string(), "date_of_birth": dt})

df = production.author.find_pandas_all({}, schema = author)
arrow_table = production.author.find_arrow_all({}, schema = author)
ndarrays = production.author.find_numpy_all({}, schema = author)

print(ndarrays)