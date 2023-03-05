from kafka import KafkaProducer
from json import dumps, load
from time import sleep

KAFKA_TOPIC_NAME = "topic"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

def produce(spark_message):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: dumps(x).encode('utf-8'), acks = 0)

    producer.send(KAFKA_TOPIC_NAME, value={"spark_message": spark_message})

if __name__ == "__main__":

        data = ["data1","data2","data3","data4","data5","data6"]

        for msg in data:
            produce(msg)
            print(msg) 
           # sleep(1)
    
