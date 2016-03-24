from elasticsearch import Elasticsearch
from elasticsearch import ConnectionError
from elasticsearch import NotFoundError
def updatefile():
    try:
        coll=Elasticsearch(['localhost:9200'])
        msg = coll.update(index='megacorp',doc_type='employee',id=1,
                    body={"doc": {"first_name": "su", "last_name": "lixin","mode":"fire" }})
        print msg
        res = coll.search(index="megacorp", body={"query": {"match": {'last_name':'lixin'}}})
        print res,type(res)
    except NotFoundError:
        print 'not exist'
    except ConnectionError:
        print 'ConnectionError'
        exit(-1)
    except Exception as e:
        print 'sth wrong'
        raise e

