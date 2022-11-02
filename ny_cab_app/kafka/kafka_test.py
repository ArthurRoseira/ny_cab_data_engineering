from kafka_client import ClientManager


client = ClientManager(topic='ny_cab_data')
client.get_producer()
client.send_data('test_data')
