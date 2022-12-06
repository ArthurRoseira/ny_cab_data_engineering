import json
import asyncio
from pymongo import MongoClient,errors
from flask import Flask, render_template, request, url_for, redirect
import requests as rt
import os
import datetime
# mongodb://[username:password@]host1[:port1]


from kafka.kafka_client import ClientManager

topic_name = 'ny_cab_data'
kafka_client = ClientManager(topic=topic_name)
kafka_client.get_producer()

port = 0

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
            data = rt.post(url=f"http://0.0.0.0/{port}/fare_amount",data={'test':'message'})
        )
        cost = await trip_cost
        print(cost)
        kafka_client.send_data(json.dumps(request.form))
        return redirect(url_for('index'))
    return(render_template('index.html',taxi_zones = list(result)))


if __name__=="__main__":
    app.run(debug=True, host='0.0.0.0')