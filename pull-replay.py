import requests
import json
from elasticsearch import Elasticsearch
from datetime import datetime
import time

def main():
    es = Elasticsearch(port=9203)
    es_index = "p2-6v0s1-100"
    batch_size = 100
    batch_number = 1

    replay_ids = get_replay_ids(es, es_index, batch_size)
    while replay_ids:
        for replay_id in replay_ids:
            replay_data = pull(replay_id)
            es.update(index=es_index, id=replay_id, body={"doc": replay_data})
        replay_ids = get_replay_ids(es, es_index, batch_size)
        print(datetime.now(), ": Batch number", batch_number)
        batch_number += 1
    print("done!")

def get_replay_ids(es, es_index, batch_size):
    body= {
        "query": {
            "bool": { 
                "must_not": {
                    "exists": {
                        "field": "replay_data"
                    }
                }
            }
        },
        "size": batch_size
    }
    res = es.search(index=es_index, body=body)
    docs = res.get('hits').get('hits')

    return [doc.get('_id') for doc in docs]

def pull(replay_id):
    try:
        r = requests.get("https://hanab.live/export/" + replay_id)
    except Exception as e:
        print("There was a problem pulling replay data for replay_id: " + replay_id)
        print(e)
        return {"error": e}
    return r.json()


if __name__ == "__main__":
    main()
