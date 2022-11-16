import json
import asyncio
from pymongo import MongoClient,errors
from flask import Flask, render_template, request, url_for, redirect
import os
import datetime
# mongodb://[username:password@]host1[:port1]


from kafka.kafka_client import ClientManager

topic_name = 'ny_cab_data'
kafka_client = ClientManager(topic=topic_name)
kafka_client.get_producer()

def trip_cost_request(form_data:dict)->list:
    data = []
    kafka_client.send_data(json.dumps(form_data))
    for i in kafka_client.get_consumer():
        data.append('data:{0}\n\n'.format(i.value.decode()))
    return data



app = Flask(__name__)

URI = "mongodb://root:example@localhost:27017/"

@app.route('/', methods=(['GET','POST']))
async def index():
    try:
        client = MongoClient(URI,serverSelectionTimeoutMS=100)
        print(client.server_info())
        result = client.ny_cab_data.taxi_zones.find({}, {"_id":0})
        collection = client.ny_cab_data.ny_cab_trips
    except errors.ServerSelectionTimeoutError as err:
        print(err)
    if request.method=='POST':
        request.form['first_name']
        trip_cost = asyncio.create_task(
            trip_cost_request({'test':'message'})
        )
        cost = await trip_cost
        print(cost)
        # collection.insert_one({
        #    'first_name':request.form['first_name'],
        #    'last_name':request.form['last_name'],
        #    'gender':request.form['gender'],
        #    'address':request.form['address'],
        #    'from_zone':request.form['from_zone'],
        #    'to_zone':request.form['to_zone'],
        #    'email':request.form['email'],
        #    'time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # })
        # client.close()
        return redirect(url_for('index'))
    return(render_template('index.html',taxi_zones = list(result)))


if __name__=="__main__":
    app.run(debug=True, host='0.0.0.0')