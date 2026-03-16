### Some aggregations.
### These are examples only.
### TODO: You want to modify? Go ahead.

## Here, this counts how many jobs were created, grouped by date
from pymongo import MongoClient

# Requires the PyMongo package.
# https://api.mongodb.com/python/current

client = MongoClient('mongodb://localhost:27017/')
result = client['transformed']['transformed'].aggregate([
    {
        '$group': {
            '_id': {
                '$dateToString': {
                    'format': '%Y-%m-%d', 
                    'date': '$created'
                }
            }, 
            'jobCounts': {
                '$sum': 1
            }
        }
    }
])

####

### Here, we can obtain the top X skills across the days

from pymongo import MongoClient

# Requires the PyMongo package.
# https://api.mongodb.com/python/current

client = MongoClient('mongodb://localhost:27017/')
result = client['transformed']['transformed'].aggregate([
    {
        '$match': {
            'created': {
                '$ne': None
            }
        }
    }, {
        '$project': {
            'created': 1, 
            'skills': 1
        }
    }, {
        '$unwind': {
            'path': '$skills'
        }
    }, {
        '$group': {
            '_id': {
                'date': {
                    '$dateToString': {
                        'format': '%Y-%m-%d', 
                        'date': '$created'
                    }
                }, 
                'skill': '$skills'
            }, 
            'count': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            '_id.date': 1, 
            'count': -1
        }
    }, {
        '$group': {
            '_id': '$_id.date', 
            'top_skills': {
                '$push': {
                    'skill': '$_id.skill', 
                    'count': '$count'
                }
            }
        }
    }, {
        '$project': {
            'top_skils': {
                '$slice': [
                    '$top_skills', 5
                ]
            }
        }
    }
])

####

# You want to see who is being active at a daily level? See no more here!!!!

from pymongo import MongoClient

# Requires the PyMongo package.
# https://api.mongodb.com/python/current

client = MongoClient('mongodb://localhost:27017/')
result = client['transformed']['transformed'].aggregate([
    {
        '$match': {
            'company': {
                '$ne': None
            }, 
            'created': {
                '$ne': None
            }
        }
    }, {
        '$group': {
            '_id': {
                'created': {
                    '$dateToString': {
                        'format': '%Y-%m-%d', 
                        'date': '$created'
                    }
                }, 
                'company': '$company'
            }, 
            'job_count': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            '_id.created': -1, 
            'job_count': -1
        }
    }, {
        '$group': {
            '_id': '$_id.created', 
            'companyLevelCounts': {
                '$push': {
                    'company': '$_id.company', 
                    'job_count': '$job_count'
                }
            }
        }
    }, {
        '$set': {
            'CompaniesPosted': {
                '$size': '$companyLevelCounts'
            }
        }
    }, {
        '$sort': {
            '_id': -1
        }
    }
])

####

# Or maybe, see the all-time most active companies!!!

from pymongo import MongoClient

# Requires the PyMongo package.
# https://api.mongodb.com/python/current

client = MongoClient('mongodb://localhost:27017/')
result = client['transformed']['transformed'].aggregate([
    {
        '$match': {
            'company': {
                '$ne': None
            }
        }
    }, {
        '$group': {
            '_id': '$company', 
            'AllTimeCount': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            'AllTimeCount': -1
        }
    }
])