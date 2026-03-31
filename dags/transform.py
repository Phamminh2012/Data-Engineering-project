def transformMCF(to_key):

    from pymongo import MongoClient

    # Requires the PyMongo package.
    # https://api.mongodb.com/python/current

    client = MongoClient("mongodb://host.docker.internal:27017")
    result = client['raw_api_result']['mcf'].aggregate([
        {
            '$project': {
                'title': 1, 
                'description': 1, 
                'postedCompany': 1, 
                'metadata': 1, 
                'skills': 1,
                'ssocCode': 1,
                'ssecEqa': 1
            }
        }, {
            '$lookup': {
                'from': 'ssoc_to_iso',
                'localField': 'ssocCode',
                'foreignField': 'SSOC',
                'as': 'iso_lookup'
            }
        }, {
            '$lookup': {
                'from': 'ssec_mappings',
                'let': { 'ssec_first_digit': { '$substr': ['$ssecEqa', 0, 1] } },
                'pipeline': [
                    { '$match': { '$expr': { '$eq': ['$SSEC_CODE', '$$ssec_first_digit'] } } }
                ],
                'as': 'ssec_lookup'
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
                'source': 'mcf',
                'iso_code': { '$arrayElemAt': ['$iso_lookup.ISCO-08', 0] },
                'education_level': { '$arrayElemAt': ['$ssec_lookup.ATTAINMENT', 0] }
            }
        }, {
            '$project': {
                'metadata': 0, 
                'postedCompany': 0,
                'iso_lookup': 0,
                'ssec_lookup': 0,
                'ssecEqa': 0
            }
        }, {
            '$merge': {
                'into': {
                    'db': 'transformed', 
                    'coll': 'transformed'
                }, 
                'on': '_id', 
                'whenMatched': 'merge', 
                'whenNotMatched': 'insert'
            }
        }
    ])

###

def transformJSearch(to_key):

    from pymongo import MongoClient

    # Requires the PyMongo package.
    # https://api.mongodb.com/python/current

    client = MongoClient("mongodb://host.docker.internal:27017")
    result = client['raw_api_result']['jsearch'].aggregate([
        {
            '$project': {
                'job_title': 1, 
                'job_description': 1, 
                'employer_name': 1, 
                'job_posted_at_datetime_utc': 1,
                'skills': 1,
                'job_onet_job_zone': 1,
                'job_onet_soc': 1
            }
        }, {
            '$lookup': {
                'from': 'onet_zone_mappings',
                'localField': 'job_onet_job_zone',
                'foreignField': 'ONET_CODE',
                'as': 'zone_lookup'
            }
        }, {
            '$lookup': {
                'from': 'ostar_to_iso',
                'let': { 'onet_formatted': { '$concat': [{ '$substr': ['$job_onet_soc', 0, 2] }, '-', { '$substr': ['$job_onet_soc', 2, 4] }] } },
                'pipeline': [
                    { '$match': { '$expr': { '$eq': ['$OSTAR-SCO', '$$onet_formatted'] } } }
                ],
                'as': 'ostar_lookup'
            }
        }, {
            '$set': {
                'created': {
                    '$dateFromString': {
                        'dateString': '$job_posted_at_datetime_utc'
                    }
                }, 
                'source': 'jsearch',
                'title': '$job_title',
                'education_level': { '$arrayElemAt': ['$zone_lookup.ATTAINMENT', 0] },
                'iso_code': { '$arrayElemAt': ['$ostar_lookup.ISCO-08', 0] }
            }
        }, {
            '$project': {
                'job_posted_at_datetime_utc': 0,
                'job_title': 0,
                'zone_lookup': 0,
                'ostar_lookup': 0
            }
        }, {
            '$merge': {
                'into': {
                    'db': 'transformed', 
                    'coll': 'transformed'
                }, 
                'on': '_id', 
                'whenMatched': 'merge', 
                'whenNotMatched': 'insert'
            }
        }
    ])