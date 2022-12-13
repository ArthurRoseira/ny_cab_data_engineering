from flask import Flask, render_template, request, jsonify
from pyspark.sql import SparkSession
from pyspark.ml.regression import GeneralizedLinearRegressionModel
from pyspark.ml.linalg import Vectors
import os 
import numpy as np
path_time = os.path.join(os.path.abspath(''),'ML model Development','model_trip_duration','bestModel')
path_amount = os.path.join(os.path.abspath(''),'ML model Development','model_fare_amount','bestModel')

spark = SparkSession.builder.appName("fare_amount").master("local").config("spark.driver.memory", "15g").getOrCreate()
time_model = GeneralizedLinearRegressionModel.load('file:///'+path_time)
fare_amount_model = GeneralizedLinearRegressionModel.load('file:///'+path_amount)

# from kafka.kafka_client import ClientManager

# topic_name = 'fare_amount_model'
# kafka_client = ClientManager(topic=topic_name)
# kafka_client.get_producer()

app = Flask(__name__)


@app.route('/fare_amount/', methods=(['GET','POST']))
def index():
    content = request.json
    app.logger.info("content")
    print(content)
    vars = ['pickup_day_shift','trip_distance','pickup_day','avg(trip_duration)']
    data =   np.array([content['value1'],content['value1'],content['value1'],content['value1']])
    time_predict = time_model.predict(Vectors.dense(data))
    fare_amount = fare_amount_model.predict(Vectors.dense([content['trip_distance'],time_predict]))
    # data = json.load(request.data)
    # print(data)
    # kafka_client.send_data(json.dumps(data))
    return jsonify({'fare_amount':time_predict})


if __name__=="__main__":
    app.run( host='0.0.0.0')