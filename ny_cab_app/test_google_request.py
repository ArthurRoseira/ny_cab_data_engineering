
import requests
from bs4 import BeautifulSoup
import re 
from pymongo import MongoClient,errors
import time

URI = "mongodb://root:example@localhost:27017/"
client = MongoClient(URI,serverSelectionTimeoutMS=100)
zones = client.ny_cab_data.taxi_zones.find({})


class DataCrawler:

    def __init__(self):
        self.usr_agent = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}


    def get_request(self,query:str)->list:
        results = []
        resp = requests.get(
        url="https://www.google.com/search",
        headers=self.usr_agent,
        params=dict(
            q = query.replace(' ', '+'),
            num = 12,
            hl = "en",
            start = 0,
        ),
        proxies=None,
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        result_block = soup.find_all('div', attrs={'class': 'g'})
        for result in result_block:
            element = result.find('h3')
            if element != None:
                title = element.text
            else:
                title = 'None'
            element  = result.find('a', href=True)
            if element != None:
                link = element['href']
            else:
                link = 'None'
            results.append({'title':title,'link':link})
        return results


    def get_coordinates(self, url_list:list)->dict:
        try:
            url = next(item for item in url_list if any(s in item["link"] for s in ['latitude.to','latlong','distancesto']))
            if 'latitude.to' in url['link']:
                coor = self.get_coordinates_latitudeto(url['link'])
            elif 'wikipedia' in url['link']:
                coor= self.get_coordinates_wikipedia(url['link'])
            elif 'latlon' in url['link']:
                coor = self.get_coordinates_latlon(url['link'])
            else:
                coor = self.get_coordinates_distancesto(url['link'])
            return coor
        except Exception as e:
            print(e)
            return {} 

    def get_coordinates_latlon(self,url)->dict:
        thepage = requests.get(url)
        soup = BeautifulSoup(thepage.text, "html.parser")
        values = soup.findAll('strong')
        return {'latitude': float(values[2].text),'longitude':float(values[3].text)}


    def get_coordinates_wikipedia(self,url)->dict:
        coordinates = {}
        thepage = requests.get(url)
        soup = BeautifulSoup(thepage.text, "html.parser")
        table = soup.find('table', attrs={'class':'margina'})
        table_rows = table.find_all('tr')
        coordinates = {}
        for tr in table_rows:
            td = tr.find_all('td')
            row = [tr.text.strip() for tr in td if tr.text.strip()]
            if 'Latitude' in tr.text:
                coordinates['latitude'] = float(row[0])
            if 'Longitude' in tr.text:
                coordinates['longitude'] = float(row[0])
        return coordinates


    def get_coordinates_latitudeto(self,url)->dict:
        coordinates = {}
        thepage = requests.get(url)
        soup = BeautifulSoup(thepage.text, "html.parser")
        value = soup.findAll(attrs={"id" : "DD"})[0]
        value = value['value']
        coordinates['latitude'] = float(value.split(' ')[0])
        coordinates['longitude'] = float(value.split(' ')[1])
        return coordinates

    def get_coordinates_distancesto(self,url):
        thepage = requests.get(url)
        soup = BeautifulSoup(thepage.text, "html.parser")
        table = soup.find('table', attrs={'class':'table table-bordered table-striped table-hover'})
        table_rows = table.find_all('tr')
        coordinates = {}
        for tr in table_rows:
            td = tr.find_all('td')
            row = [tr.text.strip() for tr in td if tr.text.strip()]
            if 'Latitude:' in tr.text:
                coordinates['latitude'] = float(row[0])
            if 'Longitude:' in tr.text:
                coordinates['longitude'] = float(row[0])
        return coordinates





if __name__ == '__main__':
    crawler = DataCrawler()
    for zone in zones:
        if 'coordinates' not in zone.keys():
            query = zone['Zone'] + ' new york coordinates'
            urls = crawler.get_request(query)
            coor = crawler.get_coordinates(urls)
            if len(coor.values())>0:
                print(zone['Zone'],coor)
                client.ny_cab_data.taxi_zones.update_one({"_id":zone['_id']},{"$set":{"coordinates":coor}})
                time.sleep(3)
            else:
                print(zone['Zone'],'NOT Found')





