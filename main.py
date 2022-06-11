from dotenv import load_dotenv, find_dotenv
import os 
import pprint
from pymongo import MongoClient
load_dotenv(find_dotenv())


# Get mongodb password from environment variable
password = os.environ.get('MONGODB_PWD')

# define connection string for mongodb - get from your mongodb database
connection_string = f'mongodb+srv://afif:{password}@tutorial.819uiqo.mongodb.net/?retryWrites=true&w=majority'

# call client to connect to mongodb
client = MongoClient(connection_string)

dbs = client.list_database_names() # get list of all databases
test_db = client.test # create a new dbs / select a dbs
collections = test_db.list_collection_names() # get list of all collections


# insert test documents
def insert_test_doc():
    collection = test_db.test
    test_document = {
        "name": "Afif",
        "type": "human"
    }
    inserted_id = collection.insert_one(test_document).inserted_id
    print(inserted_id)
    
production = client.production # create new db production
person_collection = production.person_collection # create new collection person_collection


# create documents
def create_documents():
    first_names = ['Afif', 'John', 'Jane', 'Mary', 'Bob', 'Tom', 'Sue', 'Jack', 'Jill']
    last_names = ['Al-Assad', 'Smith', 'Jones', 'Williams', 'Brown', 'Davis', 'Miller', 'Wilson', 'Moore']
    ages = [20, 21, 22, 23, 24, 25, 26, 27, 28]
    
    docs = []
    
    for first_names, last_names, ages in zip(first_names, last_names, ages):
        doc = {'first_name': first_names, 'last_name': last_names, 'age': ages}
        docs.append(doc)
        
    person_collection.insert_many(docs)


# better printer for mongodb documents
printer = pprint.PrettyPrinter()


# find all documents in collection    
def find_all_people():
    people = person_collection.find()
    
    for person in people:
        printer.pprint(person)
        

# find specific documents in collection        
def find_afif():
    afif = person_collection.find_one({'first_name': 'Afif'})
    printer.pprint(afif)
    

# count all documents in collection
def count_all_people():
    count = person_collection.count_documents({})
    print(count)
    

# get document by id   
def get_person_by_id(person_id):
    from bson.objectid import ObjectId
    
    _id = ObjectId(person_id)
    person = person_collection.find_one({'_id': _id})
    printer.pprint(person)
    

# get document by specific field   
def get_age_range(min_age, max_age):
    query = {'$and':[
                {'age': {'$gte': min_age, '$lte': max_age}}
    ]}
    people = person_collection.find(query).sort('age')
    for person in people:
        printer.pprint(person)
        

# get specific field from all documents     
def project_columns():
    columns = {'_id': 0, 'first_name': 1, 'last_name': 1, 'age': 1}
    people = person_collection.find({}, columns)
    for person in people:
        printer.pprint(person)
        

# update document by id       
def update_person_by_id(person_id):
    from bson.objectid import ObjectId
    
    _id = ObjectId(person_id)
    
    # all_updates = {
    #     "$set": {"new_field": True},
    #     "$inc": {"age": 1},
    #     "$rename": {"first_name": "first", "last_name": "last"}
    #     }
    
    # person_collection.update_one({'_id': _id}, all_updates)
    
    person_collection.update_one({'_id': _id}, {'$set': {'age': 30}})
    

# replace field in document by id
def replace_one(person_id):
    from bson.objectid import ObjectId
    
    _id = ObjectId(person_id)
    
    person = {
        '_id': _id,
        'first_name': 'Afif',
        'last_name': 'faiz',
        'age': 30
    }
    
    person_collection.replace_one({'_id': _id}, person)
    

# delete document by id
def delete_doc_by_id(person_id):
    from bson.objectid import ObjectId
    
    _id = ObjectId(person_id)
    
    person_collection.delete_one({'_id': _id})


# --------------------------------------------------

address = {
    "_id" : "5e8f8f8f8f8f8f8f8f8f8f8",
    "street" : "123 Main St.",
    "number" : 123,
    "city" : "Anytown",
    "country" : "USA",
    "zip" : "12345",
}


# add field to document
def add_address_embed(person_id, address):
    from bson.objectid import ObjectId
    
    _id = ObjectId(person_id)
    
    person_collection.update_one({'_id': _id}, {'$addToSet': {'addresses': address}})
    

# add field to document by connecting to other document in other collection
def add_address_relationship(person_id, address):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)
    
    address = address.copy()
    address['owner_id'] = person_id
    
    address_collection = production.address
    address_collection.insert_one(address)