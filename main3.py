from unittest import result
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


jeoprady_db = client.jeoprady_db
question = jeoprady_db.question


def fuzzy_matching():
    result = question.aggregate([
        {
            "$search": {
                "index": "language_search",
                "text": {
                    "query": "computer",
                    "path": "category",
                    "synonyms": "mapping"
                }
            }
        }
    ])

    printer.pprint(list(result))
    

def autocomplete():
    result = question.aggregate([
        {
            "$search": {
                "index": "language_search",
                "autocomplete": {
                   "query": "computer progammer",
                   "path": "question",
                   "tokenOrder": "sequential",
                   "fuzzy": {}
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "question": 1
            }
        }
    ])

    printer.pprint(list(result))
    
    
def compound_queries():
    result = question.aggregate([
        {
            "$search": {
                "index": "language_search",
                "compound": {
                    "must": [
                        {
                            "text": {
                                "query": ["COMPUTER", "CODING"],
                                "path": "category",
                            }
                        }
                    ],
                    "mustNot": [{
                        "text": {
                            "query": "codes",
                            "path": "category",
                        }
                    }],
                    "should": [{
                        "text": {
                            "query": "application",
                            "path": "answer"
                        }
                    }]
                }
            }
        },
        {
            "$project": {
                "question": 1,
                "answer": 1,
                "category": 1,
                "score": {"$meta": "searchScore"}
            }
        }
    ])
    
    printer.pprint(list(result))
    

def relevance():
    
    result = question.aggregate([
        {
            "$search": {
                "index": "language_search",
                "compound": {
                    "must": [
                        {
                            "text": {
                                "query": "geography",
                                "path": "category"
                            }
                        },
                    ],
                    "should": [
                        {
                            "text": {
                                "query": "Final Jeopardy",
                                "path": "round",
                                "score": {"boost": {"value": 3.0}}
                            }
                        },
                        {
                            "text": {
                                "query": "Double Jeopardy",
                                "path": "round",
                                "score": {"boost": {"value": 2.0}}
                            }
                        }
                    ]
                }
            }
        },{
        "$project": {
            "question": 1,
            "answer": 1,
            "category": 1,
            "round": 1,
            "score": {"$meta": "searchScore"}
        }
        },{
            "$limit": 10
        }
    ])
    
    printer.pprint(list(result))

relevance()