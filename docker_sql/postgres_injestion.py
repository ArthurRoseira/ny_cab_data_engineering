from urllib import request
import os 
import argparse
from pyspark.sql import SparkSession

import findspark
findspark.init()
findspark.find()
import pandas as pd
findspark.find()

class PostgresIngestion:

    def __init__(self,params,spark) -> None:
        self.user = params.user
        self.password = params.password
        self.host = params.host 
        self.port = int(params.port) 
        self.db = params.db
        self.start_year = int(params.start_year)
        self.end_year = int(params.end_year)
        self.spark = spark
        self.yellow_cab_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{}.parquet"
        self.taxi_zones_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
        self.download_path = os.path.join(os.path.dirname(__file__),"Files") 
        
        
    def yellow_cab_data(self):
        start_year = int(self.start_year)
        end_year = int(self.end_year)
        if start_year == end_year: 
            ym_list = zip([start_year],range(1,12,1))
        else:
            ym_list = zip(range(start_year,end_year,1),range(1,12,1))
        log = {}
        print(ym_list)
        for ym in ym_list:
            date_month = str(ym[0]) + "-" + str(ym[1]).zfill(2) 
            path =  os.path.join(self.download_path, f"yellow_tripdata_{date_month}.parquet")
            url = self.yellow_cab_url.format(date_month)
            try:
                request.urlretrieve(url, path)
                data = self.spark.read.parquet(path)
                print('File Downloaded:',date_month,str([data.count(), len(data.columns)]))
                # print(pd.DataFrame(columns=['id'],data = [1]).head())
                # data = spark.createDataFrame(pd.DataFrame(columns=['id'],data = [1]))
                postgres_url = f"jdbc:postgresql://{self.host}:{self.port}/{self.db}"
                print(postgres_url)
                data.write.format('jdbc').options(
                url=postgres_url,
                driver="org.postgresql.Driver",
                dbtable='ny_taxi',
                user='root',
                password='root'
                ).save()
                log[date_month] = str([data.count(), len(data.columns)])
            except Exception as e:
                print(e)

        
    def taxi_zones_data(self):
        log = {}
        path =  os.path.join(self.download_path, "taxi_zones.csv")
        try:
            request.urlretrieve(self.taxi_zones_url, path)
            data = self.spark.read.csv(path)
            print('File Downloaded:',str([data.count(), len(data.columns)]))
            postgres_url = f"jdbc:postgresql://{self.host}:{self.port}/{self.db}"
            print(postgres_url)
            data.write.format('jdbc').options(
                url=postgres_url,
                driver="org.postgresql.Driver",
                dbtable='ny_taxi_zones',
                user='root',
                password='root'
                ).save()
        except Exception as e:
            print(e)



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Postgres Data Ingestion')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--start_year', required=True, help='Start date year for injestion')
    parser.add_argument('--end_year', required=True, help='End date year for injestion')

    args = parser.parse_args()

    # class args:
    #     def __init__(self,user,password,host,port,db,start_year,end_year,spark) -> None:
    #         self.user = user
    #         self.password = password
    #         self.host = host 
    #         self.port = port
    #         self.db = db
    #         self.start_year = start_year
    #         self.end_year = end_year
    #         self.spark = spark

    spark = SparkSession.builder.appName("PostgresInjection").config("spark.jars", os.path.join(os.path.dirname(__file__),'postgresql-42.5.0.jar')).getOrCreate()
    #args_ = args('root','root','localhost',5432,"ny_taxi",2022,2022,spark)
    ingestion = PostgresIngestion(args,spark)
    #ingestion.yellow_cab_data()
    ingestion.taxi_zones_data()
    spark.stop()

