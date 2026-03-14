import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
	'weather-data',
	bootstrap_servers = 'localhost: 9092',
	auto_offset_reset = 'latest',
	value_deserializer = lambda m: json.loads(m.decode('utf-8'))
)
print("------------Real Time Weather Alert System--------------")
print("Waiting for data")
for message in consumer:
	report = message.value
	city =  report['city']
	temp =  report['temp']
	if temp > 40:
		print(f"Alert: Critical Heat in  {city} : {temp} C ")
	else:
		print(f"The {city}  is under normal teamperature")
		
