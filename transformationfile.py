from pymongo import MongoClient

# Requires the PyMongo package.
# https://api.mongodb.com/python/current

client = MongoClient('mongodb://localhost:27017/')
result = client['raw_api_result']['mcf'].aggregate([
    {
        '$project': {
            'title': 1, 
            'description': 1, 
            'postedCompany': 1, 
            'metadata': 1
        }
    }, {
        '$set': {
            'created': {
                '$dateFromString': {
                    'dateString': '$metadata.createdAt'
                }
            }, 
            'company': '$postedCompany.name', 
            'source': 'mcf'
        }
    }, {
        '$project': {
            'metadata': 0, 
            'postedCompany': 0
        }
    }, {
        '$out': {
            'db': 'transformed', 
            'coll': 'transformed'
        }
    }
])

######

from pymongo import MongoClient

# Requires the PyMongo package.
# https://api.mongodb.com/python/current

client = MongoClient('mongodb://localhost:27017/')
result = client['raw_api_result']['adzuma'].aggregate([
    {
        '$project': {
            'title': 1, 
            'description': 1, 
            'company': 1, 
            'created': 1
        }
    }, {
        '$set': {
            'created': {
                '$dateFromString': {
                    'dateString': '$created'
                }
            }, 
            'company': '$company.display_name', 
            'source': 'adzuna'
        }
    }, {
        '$out': {
            'db': 'transformed', 
            'coll': 'transformed'
        }
    }
])

