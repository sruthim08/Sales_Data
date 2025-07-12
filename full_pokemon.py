import requests as r
import pandas as pd
from time import sleep
import mysql.connector
from mysql.connector import Error



url="https://pokeapi.co/api/v2/pokemon?limit=10/"
dict_dbConnect={"host":"localhost","port":"3306","user":"root","password":"Sruthi@1809","database":"dvdrentals"}

def extract(url):
    try:
        response=r.get(url,timeout=10)
        response.raise_for_status()
        data=response.json() #converts data into json
        #extracting the base url for each pokemon#
        return[pokemon['url'] for pokemon in data['results']] #getting url from api under results
    
    except Exception as e:
        print(f"{e}failed")
#a=extract(url)
#print(a)

def getData(url):
    try:
        response=r.get(url,timeout=10)
        response.raise_for_status()
        data1=response.json()
        return {"name":data1['name'],  
               "baseexp":data1['base_experience'],
               "height":data1['height'],
               "weight":data1['weight'],
               "types": ','.join([t["type"]["name"] for t in data1["types"]]),
               "abilities": ','.join([a["ability"]["name"] for a in data1["abilities"]]),
               "sprite": data1["sprites"]["front_default"]
 }


    except Exception as e:
        print(f"{e}failed")

'''def transform(data):
    try:
        results=data.get('results',[])
        pokemon_details=[{
            "name":item['name'],
            "url":item['url']
        }for item in results]
        return pd.DataFrame(pokemon_details)
    except (KeyError,TypeError)as e:
        print(f"{e}failed")
        return pd.DataFrame()'''



'''def load(df:pd.DataFrame,):
    try:
        if df.empty:
            print("No data to load into CSV")
            return 
        df.to_csv(file_name,index=False)
        print(f"Data successfully loaded into {file_name}")
    except Exception as e:
        print(f"Error loading data into CSV: {e}")'''

def load(data):
    '''if data.empty:
        print("DataFrame is empty. Nothing to load.")
        return'''
    try:
        conn=mysql.connector.connect(**dict_dbConnect)
        cursor=conn.cursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS pokemon_data (
                id SERIAL PRIMARY KEY,
                name TEXT,
                base_experience INT,
                height INT,
                weight INT,
                types TEXT,
                abilities TEXT,
                sprite TEXT
            );
        """)
        insert_query="""INSERT INTO pokemon_data (name, base_experience, height, weight, types, abilities, sprite)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            
        """
        for row in data:
            cursor.execute(insert_query,(row['name'],row['baseexp'],row['height'],row['weight'],row['types'],row['abilities'],row['sprite']))
        conn.commit()
        print(f"Successfully inserted {cursor.rowcount} new rows.")
    except Error as e:
        print(f"Database error: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("MySQL connection is closed")

               
                        
        



def main():
    allpokemon_url=extract(url) #saving all url from extract
    pokemon_data=[] #creating empty list
    for pokemon_url in allpokemon_url: 
        abc=getData(pokemon_url)
        if abc:
            pokemon_data.append(abc)
            sleep(0.1)
    if pokemon_data:
        pokemon_df=pd.DataFrame(pokemon_data)
        pokemon_df.to_csv("pokemon_full.csv",index=False)
        print("saved to csv")
        load(pokemon_data)
    else:
        print("no data is saved")


if __name__ == "__main__":
    main()





    '''data = extract(url)
    pokemon_df = transform(data)
    load(pokemon_df)    
    print(pokemon_df)'''





