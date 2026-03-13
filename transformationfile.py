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
            'metadata': 1, 
            'skills': 1
        }
    }, {
        '$set': {
            'created': {
                '$dateFromString': {
                    'dateString': '$metadata.createdAt'
                }
            }, 
            'company': '$postedCompany.name', 
            'skills': '$skills.skill', 
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

######

from pymongo import MongoClient

# Requires the PyMongo package.
# https://api.mongodb.com/python/current

client = MongoClient('mongodb://localhost:27017/')
result = client['raw_api_result']['jsearch'].aggregate([
    {
        '$project': {
            'job_title': 1, 
            'job_description': 1, 
            'eployer_name': 1, 
            'job_posted_at_datetime_utc': 1
        }
    }, {
        '$set': {
            'created': {
                '$dateFromString': {
                    'dateString': '$job_posted_at_datetime_utc'
                }
            }
        }
    }, {
        '$project': {
            'job_posted_at_datetime_utc': 0
        }
    }, {
        '$out': {
            'db': 'transformed', 
            'coll': 'transformed'
        }
    }
])