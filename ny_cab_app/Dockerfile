FROM python:3.8

WORKDIR /app
COPY ny_cab_trips_api.py ny_cab_trips_api.py
COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

ENTRYPOINT ["python","ny_cab_trips_api.py"]