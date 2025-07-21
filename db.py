from pymongo import MongoClient
from bson import ObjectId

#local imports
import config


def get_client():
    client = MongoClient(config.DB_URI)
    return client


class DBHelperClass:
    def __init__(self):
        # self.client = get_client()
        # self.my_db = self.client.get_database()  # Access the default database
        self.client = None
        self.my_db = None
        
    def connect(self):
        if not self.client:  # Create client only if it doesn't exist
            self.client = MongoClient(config.DB_URI)
            self.my_db = self.client.get_database()

        
    # will give all record from a collection, also take where to filter
    def get_all_record(self, collection_name = None, filter = {}):
        self.connect()
        try:
            my_collection = self.my_db[collection_name]
            # all_rec = my_collection.find(filter, {"_id":0})
            all_rec = my_collection.find(filter)
            return all_rec
        except Exception as e:
            print(f"error while get_all_record collection_name= {collection_name}, filter= {filter}, error = {e}")
            return []
        
    def get_count(self, collection_name = None, filter = {}):
        self.connect()
        try:
            my_collection = self.my_db[collection_name]
            count = my_collection.count_documents(filter)
            return count
        except Exception as e:
            print(f"error while get_count collection_name= {collection_name}, filter= {filter}, error = {e}")
            return 0
    
    def get_one_record(self, collection_name = None, filter = {}):
        self.connect()
        try:
            my_collection = self.my_db[collection_name]
            document = my_collection.find_one(filter)
            return document
        except Exception as e:
            print(f"error while get_one_record collection_name= {collection_name}, filter= {filter}, error = {e}")
            return {}
        
    def get_one_record_by_id(self, collection_name = None, id = None):
        self.connect()
        try:
            my_collection = self.my_db[collection_name]
            filter_criteria = {'_id': ObjectId(id)}
            document = my_collection.find_one(filter_criteria)
            return document
        except Exception as e:
            print(f"error while get_one_record collection_name= {collection_name}, filter= {filter}, error = {e}")
            return {}
    
    # update one record on the basis of _id
    def update_one_record_by_id(self, collection_name = None, id = None, new_values = {}):
        self.connect()
        try:
            if not id:
                return
            my_collection = self.my_db[collection_name]
            new_values = {"$set": new_values}
            filter_criteria = {'_id': ObjectId(id)}
            doc = my_collection.update_one(filter_criteria, new_values)
        except Exception as e:
            print(f"error while update_one_record collection_name= {collection_name}, id= {id}, new_values= {new_values}, error = {e}")
            return None

    def update_one_record_by_query(self, collection_name = None, filter_criteria = {}, new_values = {}):
        self.connect()
        try:
            my_collection = self.my_db[collection_name]
            new_values = {"$set": new_values}
            doc = my_collection.update_one(filter_criteria, new_values)
        except Exception as e:
            print(f"error while update_one_record collection_name= {collection_name}, id= {id}, new_values= {new_values}, error = {e}")
            return None

    def update_many_record(self, collection_name = None, filter_criteria = {}, new_values = {}):
        self.connect()
        try:
            my_collection = self.my_db[collection_name]
            new_values = {"$set": new_values}
            my_collection.update_many(filter_criteria, new_values)
        except Exception as e:
            print(f"error while update_many_record collection_name= {collection_name}, filter_criteria= {filter_criteria}, new_values= {new_values}, error = {e}")
            return None

    def insert_one_record(self, collection_name = None, document = {}):
        self.connect()
        try:
            my_collection = self.my_db[collection_name]
            inserted_document = my_collection.insert_one(document)
            return inserted_document.inserted_id
        except Exception as e:
            print(f"error while insert_one_record collection_name= {collection_name}, document= {document}, error = {e}")
            return None
        
    def insert_many_record(self, collection_name = None, documents = []):
        self.connect()
        try:
            my_collection = self.my_db[collection_name]
            inserted_documents = my_collection.insert_many(documents)
            inserted_id = inserted_documents.inserted_ids 
            return inserted_id
        except Exception as e:
            print(f"error while insert_many_record collection_name= {collection_name}, documents= {documents}, error = {e}")
            return []
        
    def delete_many_records(self, collection_name = None, condition = {}):
        self.connect()
        try:
            my_collection = self.my_db[collection_name]
            my_collection.delete_many(condition)
            return True
        except Exception as e:
            print(f"error while deleting collection_name= {collection_name}, condition= {condition}, error = {e}")
        return False

    def close_connection(self):
        self.client.close()