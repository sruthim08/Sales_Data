import requests as r
import pandas as pd


url="https://pokeapi.co/api/v2/"
ep="pokemon/ditto"    #the table that we want

def extract(url,ep):
    try:
        response=r.get(f"{url}{ep}",timeout=10)
        if response.status_code==200:
            return response.json()
        else:
            print(f"the error of response is {response.status_code}")
    except Exception as e:
        print(f"{e}failed")


a=extract(url,ep)
print(a['abilities'][1]['ability']['name'])



    

#response=r.get(url+ep)
#print(response)
#data=response.json()
#print(data)
#f"{base_url}{endpoint}"