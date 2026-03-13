# Feel free to go onto MongoDB Compass and adjust this to your needs
# For example, do you want to count based on the date?
# This is for reference only.

from datetime import datetime, tzinfo, timezone
from pymongo import MongoClient

# Requires the PyMongo package.
# https://api.mongodb.com/python/current

client = MongoClient('mongodb://localhost:27017/')
result = client['transformed']['transformed'].aggregate([
    {
        '$project': {
            'skills': 1
        }
    }, {
        '$unwind': {
            'path': '$skills'
        }
    }, {
        '$group': {
            '_id': '$skills', 
            'skill_count': {
                '$sum': 1
            }
        }
    }, {
        '$group': {
            '_id': {
                '$dateToString': {
                    'format': '%Y-%m-%d', 
                    'date': datetime.utcnow()
                }
            }, 
            'all_counts': {
                '$push': {
                    'skill': '$_id', 
                    'count': '$skill_count'
                }
            }
        }
    }, {
        '$merge': {
            'into': {
                'db': 'final', 
                'coll': 'skillcounts'
            }, 
            'on': '_id', 
            'whenMatched': 'replace', 
            'whenNotMatched': 'insert'
        }
    }
])