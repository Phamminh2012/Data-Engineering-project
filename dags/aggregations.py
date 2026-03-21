# Aggregations Operators!
# After some feedback, this isn't really so sophisicated...
# That being said, it's still a work in progress.
MDB_LINK = "mongodb://host.docker.internal:27017"

def do_job_count(whatever):
    from pymongo import MongoClient

    # Requires the PyMongo package.
    # https://api.mongodb.com/python/current

    client = MongoClient(MDB_LINK)
    result = client['transformed']['transformed'].aggregate([
        {
            '$match': {
                'created': {
                    '$ne': None
                }
            }
        }, {
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
        }, {
            '$sort': {
                '_id': -1
            }
        }, {
            '$merge': {
                'into': {
                    'db': 'final', 
                    'coll': 'jobCount'
                }, 
                'on': '_id', 
                'whenMatched': 'merge', 
                'whenNotMatched': 'insert'
            }
        }
    ])

def do_skills_count(whatever):

    from pymongo import MongoClient

    # Requires the PyMongo package.
    # https://api.mongodb.com/python/current

    client = MongoClient(MDB_LINK)
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
            '$project': {
                'skills': {
                    '$toLower': '$skills'
                }, 
                'created': 1
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
        }, {
            '$sort': {
                '_id': -1
            }
        }, {
            '$merge': {
                'into': {
                    'db': 'final', 
                    'coll': 'top5Skills'
                }, 
                'on': '_id', 
                'whenMatched': 'merge', 
                'whenNotMatched': 'insert'
            }
        }
    ])