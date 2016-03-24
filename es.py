from elasticsearch import Elasticsearch
from elasticsearch import ConnectionError
from elasticsearch import NotFoundError
import codecs
from functools import partial
import sys
from elasticsearch import helpers


def insert_id_score(infile):
    es = Elasticsearch(["localhost:9200"])
    with codecs.open(infile, 'r', 'utf-8') as infp:
        for line in infp:
            if not line.strip():
                continue
            row = line.strip().split('\t')
            poiid = row[0]
            raw_cscore = int10(row[1])
            cscore = float(row[2])
            score = {}
            score['raw_cscore'] = raw_cscore
            score['cscore'] = cscore
            es.create(index=indexname, doc_type=typename, id=poiid, body=score)


def updatefile(infile):
    poiid = 1
    body = {}
    score = {}
    score['raw_cscore'] = 1
    score['cscore'] =1.000
    body["doc"] = score
    print body
    try:
        coll = Elasticsearch(['localhost:9200'])
        msg = coll.update(index=indexname, doc_type=typename, id=1, body=body)
        print msg
        res = coll.search(index="megacorp", body={"query": {"match": {'last_name':'lixin'}}})
        print res, type(res)
    except NotFoundError:
        print 'not exist'
    except ConnectionError:
        print 'ConnectionError'
        exit(-1)
    except Exception as e:
        print 'sth wrong'
        raise e


def batch_update(infile, num):
    try:
        es = Elasticsearch(['localhost:9200'])
        with codecs.open(infile, 'r', 'utf-8') as infp:
            infos = []
            for line in infp:
                if not line.strip():
                    continue
                row = line.strip().split('\t')
                poiid = row[0]
                raw_cscore = 1#int10(row[1])
                cscore = 1.0#float(row[2])
                info = {}
                score = {}
                score['raw_cscore'] = raw_cscore
                score['cscore'] = cscore
                info['_op_type'] = 'update'
                info['_index'] = indexname
                info['_type'] = typename
                info['_id'] = poiid
                info['doc'] = score
                infos.append(info)
                if len(infos) == num:
                    helpers.bulk(es, infos)
                    del infos[0:len(infos)]
    except NotFoundError:
        print 'not exist'
    except ConnectionError:
        print 'ConnectionError'
        exit(-1)
    except Exception as e:
        print 'sth wrong'
        raise e

if __name__ == "__main__":
    infile = sys.argv[1]
    indexname = "didi_poi_v1"
    typename = "didi_score"
    int10 = partial(int, base=10)
    insert_id_score(infile)
    #updatefile(infile)

