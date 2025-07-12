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
#print(a['abilities'][1]['ability']['name'])


#transform: def-> transform : panda df  , try , assign the ability to variable

def transform(data):
    if not data or "abilities" not in data:
        print("Could not find abilities in the provided data.")
        return pd.DataFrame()

    abilities_list = []
    # Loop through the list of abilities from the JSON data
    for ability_item in data.get("abilities", []):
        # For each ability, create a dictionary with the desired info
        ability_details = {
            "name": ability_item.get("ability", {}).get("name"),
            "url": ability_item.get("ability", {}).get("url"),
            "is_hidden": ability_item.get("is_hidden"),
            "slot": ability_item.get("slot"),
        }
        abilities_list.append(ability_details)

    # Create a DataFrame from the list of dictionaries
    return pd.DataFrame(abilities_list)


#transform: def-> transform : panda df  , try , assign the ability to variable
# Main script execution
if __name__ == "__main__":
    data = extract(url,ep)
    if data:
        abilities_df=transform(data)
        print(abilities_df)





