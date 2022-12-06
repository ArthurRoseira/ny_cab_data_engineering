import json
from flask import Flask, render_template, request, url_for, redirect
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("fare_amount").master("local").config("spark.driver.memory", "15g").getOrCreate()

from kafka.kafka_client import ClientManager

topic_name = 'fare_amount_model'
kafka_client = ClientManager(topic=topic_name)
kafka_client.get_producer()

app = Flask(__name__)


@app.route('/fare_amount/', methods=(['GET','POST']))
def fare_amount():
    data = json.load(request.data)
    print(data)
    kafka_client.send_data(json.dumps(data))
    return 10


if __name__=="__main__":
    app.run(debug=True, host='0.0.0.0')