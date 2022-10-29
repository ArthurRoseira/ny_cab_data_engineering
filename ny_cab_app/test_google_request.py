
from googlesearch import search
import requests
from bs4 import BeautifulSoup
import re 
from pymongo import MongoClient,errors

URI = "mongodb://root:example@localhost:27017/"
client = MongoClient(URI,serverSelectionTimeoutMS=100)
zones = client.ny_cab_data.taxi_zones.find({})


def google_scrape_latlon(url):
    poles_mask  = ['[0-9]{2}\°\s[0-9]{2}\'\s[0-9]{2}\.[0-9]{4}\'\'\s[A-Z]','[0-9]{2}\°\s[0-9]{2}\'\s[0-9]{1}\.[0-9]{4}\'\'\s[A-Z]','[0-9]{2}\°\s[0-9]{1}\'\s[0-9]{1}\.[0-9]{4}\'\'\s[A-Z]','[0-9]{2}\°\s[0-9]{1}\'\s[0-9]{2}\.[0-9]{4}\'\'\s[A-Z]']
    coordinates = {}
    thepage = requests.get(url)
    soup = BeautifulSoup(thepage.text, "html.parser")
    coordinates_strings  = re.findall('[0-9]{2}\.[0-9]{6}',soup.text)[:2]
    for mask in poles_mask:
        poles_mask.remove(mask)
        poles =  [cor[-1] for cor in re.findall(mask,soup.text)[:2]]
        if len(poles)>0:
            if poles[0] == poles[1]:
                poles_short = []
                for mask in poles_mask:
                    poles_short =  [cor[-1] for cor in re.findall(mask,soup.text)[:1]]
                    if len(poles_short)==1:
                        if poles_short[0] in ['N','S']:
                            poles[0]=poles_short[0]
                        else:
                            poles[1]=poles_short[0]
                        break 
    for i,c in enumerate(coordinates_strings):
        if i == 0 and poles[i] == 'S':
            coordinates['latitude'] = float(c) * -1
        elif i == 0 and poles[i] == 'N':
            coordinates['latitude'] = float(c)
        elif i == 1 and poles[i] == 'W':
            coordinates['longitude'] = float(c) * -1
        else:
            coordinates['longitude'] = float(c)  
    return coordinates


def google_scrape_wikipedia(url):
    latlon_masks = ['[0-9]{2}\.[0-9]{6}']
    coordinates = {}
    thepage = requests.get(url)
    soup = BeautifulSoup(thepage.text, "html.parser")
    coordinates_strings  = re.findall('[0-9]{2}\.[0-9]{3}\°[A-Z]\s[0-9]{2}\.[0-9]{3}\°[A-Z]',soup.text)[0]
    coordinates_strings = coordinates_strings.split(" ")
    poles =  [cor[-1] for cor in coordinates_strings]
    for i,c in enumerate(coordinates_strings):
        if i == 0 and poles[i] == 'S':
            coordinates['latitude'] = float(c.split('°')[0]) * -1
        elif i == 0 and poles[i] == 'N':
            coordinates['latitude'] = float(c.split('°')[0])
        elif i == 1 and poles[i] == 'W':
            coordinates['longitude'] = float(c.split('°')[0]) * -1
        else:
            coordinates['longitude'] = float(c.split('°')[0])  
    return coordinates

for zone in zones:
    if 'coordinates' not in zone.keys():
        query = zone['Zone'] + ' coordinates'
        for url in search(query):
            if 'latlong.net' in url:
                a = google_scrape_latlon(url)
                print(a)
                client.ny_cab_data.taxi_zones.update_one({"_id":zone['_id']},{"$set":{"coordinates":a}})
                break
            if 'en.wikipedia' in url:
                try:
                    a = google_scrape_wikipedia(url)
                    print(a)
                    client.ny_cab_data.taxi_zones.update_one({"_id":zone['_id']},{"$set":{"coordinates":a}})
                    break
                except:
                    pass