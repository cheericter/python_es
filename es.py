from elasticsearch import Elasticsearch
from elasticsearch import ConnectionError
from elasticsearch import NotFoundError
from elasticsearch.helpers import BulkIndexError
import codecs
from functools import partial
import sys
from elasticsearch import helpers
import random


def insert_id_score(infile):
    es = Elasticsearch(["localhost:9200"])
    with codecs.open(infile, 'r', 'utf-8') as infp:
        cnt=0
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
            cnt += 1
            score['time'] = cnt
            es.create(index=indexname, doc_type=typename, id=poiid, body=score)


def updatefile(infile):
    poiid = 1
    body = {}
    score = {}
    score['raw_cscore'] = 1
    score['cscore'] = 1.000
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


def infoparse(line):
    #print line
    row = line.strip().split('\t')
    raw_cscore = int10(row[0])
    cscore = float(row[1])
    score = {}
    score['raw_cscore'] = raw_cscore
    score['cscore'] = cscore
    #score['bulkcnt'] = 'test'
    #print scores
    return score


def batch_update(infile, num, indexname, typename):
    try:
        es = Elasticsearch(['localhost:9200'])
        with codecs.open(infile, 'r', 'utf-8') as infp:
            infos = []
            for line in infp:
                if not line.strip():
                    continue
                row = line.strip().split('\t', 1)
                poiid = row[0]
                if random.random() > 0.999:
                    print infos
                    print poiid
                    poiid += '**'
                doc = infoparse(row[1])
                info = {}
                info['_op_type'] = 'update'
                info['_index'] = indexname
                info['_type'] = typename
                info['_id'] = poiid
                info['doc'] = doc
                infos.append(info)
                if len(infos) == num:
                    try:
                        msg = helpers.bulk(es, infos)
                        del infos[0:len(infos)]
                    except BulkIndexError as e:
                        print e
                        print msg
                        pass
    except NotFoundError:
        print 'not exist'
    except ConnectionError:
        print 'ConnectionError \n exit \n'
        exit(-1)
    except Exception as e:
        print 'sth wrong'
        raise e

if __name__ == "__main__":
    infile = sys.argv[1]
    indexname = "didi_poi_v1"
    typename = "didi_score"
    int10 = partial(int, base=10)
    #insert_id_score(infile)
    batch_update(infile, 500, indexname, typename)
