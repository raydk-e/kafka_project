import json, time , random
from kafka import KafkaProducer

producer = KafkaProducer(
	bootstrap_servers = 'localhost: 9092',
	value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
print("Sensor started sending data  .....")
while True:
	data = {'city': 'London', 'temp' : random.randint(20, 45)}
	producer.send("weather-data", value = data)
	print(f"sent to Kafka: {data}")
	time.sleep(1)
 
