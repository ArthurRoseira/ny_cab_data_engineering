import pandas as pd
from pyspark.sql import SparkSession
import psycopg2 as pg
import numpy as np 
import warnings
import os
warnings.filterwarnings('ignore')
pd.options.plotting.backend = "plotly"


if __name__=="__main__":
    postgres_url = f"jdbc:postgresql://localhost:5432/ny_taxi"
    jar_path = 'C:\\Users\\Olist\\OneDrive\\Ambiente de Trabalho\\Projects\\ny_cab_app\\docker_sql\\postgresql-42.5.0.jar'
    spark = SparkSession.builder.appName("ML_model").config("spark.jars", jar_path).config("spark.driver.memory", "15g").master('local').getOrCreate()
    df = spark.read.format("jdbc").options(
                url=postgres_url,
                driver="org.postgresql.Driver",
                dbtable='ny_taxi',
                user='root',
                password='root'
                ).load()
    df.show(5)