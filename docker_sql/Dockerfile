FROM jupyter/pyspark-notebook
RUN pip install pyspark
RUN pip install findspark

WORKDIR /app
COPY postgres_injestion.py postgres_injestion.py 
COPY postgresql-42.5.0.jar postgresql-42.5.0.jar
RUN mkdir Files 
RUN chmod -R 777 Files
EXPOSE 5432
ENTRYPOINT [ "python3","postgres_injestion.py","--user=root","--password=root","--host=docker_sql_pgdatabase_1","--port=5432","--db=ny_taxi","--start_year=2022","--end_year=2022"]
# ENTRYPOINT [ "bash" ]