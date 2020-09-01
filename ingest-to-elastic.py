import requests
import json
from elasticsearch import Elasticsearch
from datetime import datetime

def main():

    #players = [2, 3, 4, 5, 6]
    #varients = [0, 1, 2, 3]
    #max_seed = 10

    players = [2, 3, 4, 5, 6]
    varients = [0]
    max_seed = 100
    
    es = Elasticsearch(port=9203)
    clear_es_indices(es, ['p2-6v0s1-100'])

    for seed in range(1, max_seed + 1):
        for player in players:
            for varient in varients:
                seed_string = "p" + str(player) + "v" + str(varient) + "s" + str(seed)
                print(str(datetime.now()) + ": Getting metadata for seed: " + seed_string)
                try:
                    r = requests.get("https://hanab.live/seed/" + seed_string + "?api")
                except Exception as e:
                    print("There was a problem with this seed. Too many results?")
                    print(str(e))
                    continue
                seed_games = r.json()
                for game in seed_games:
                    es.index(index='p2-6v0s1-100', id=game['id'], body=game)

def clear_es_indices(es, indices):
    for index in indices:
        es.indices.delete(index=index, ignore=[400,404])
        es.indices.create(index=index)


if __name__ == "__main__":
    main()
