from pymongo import MongoClient,errors
from flask import Flask, render_template, request, url_for, redirect
import os

# mongodb://[username:password@]host1[:port1]

app = Flask(__name__)

URI = "mongodb://root:example@localhost:27017/"

@app.route('/', methods=(['GET','POST']))

def index():
    try:
        client = MongoClient(URI,serverSelectionTimeoutMS=100)
        print(client.server_info())
        result = client.ny_cab_data.taxi_zones.find({})
        collection = client.ny_cab_data.ny_cab_trips
    except errors.ServerSelectionTimeoutError as err:
        print(err)
    if request.method=='POST':
        content = request.form['content']
        degree = request.form['degree']
        collection.insert_one({'content': content, 'degree': degree})
        client.close()
        return redirect(url_for('index'))
    return(render_template('index.html',taxi_zones = result))




if __name__=="__main__":
    app.run(debug=True, host='0.0.0.0')