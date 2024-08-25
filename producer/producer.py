import json
import random
import time
from kafka import KafkaProducer
from faker import Faker

# Initialize Faker to generate random data
fake = Faker()

# Kafka configuration
KAFKA_TOPIC = 'orders'
KAFKA_SERVER = 'localhost:19092'

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def generate_user_id():
    # Generate random data for user_id
    country = random.choice(['ukraine', 'brazil', 'usa', 'france'])
    city = random.choice(['lviv', 'kyiv', 'ternopil'])
    address = random.choice(['melnika.st', 'bandery.st', 'glyboka.st', 'kyivska.st']) + str(random.randint(10, 20))
    unique_id = fake.random_number(digits=3, fix_len=True)
    return f"{country}/{city}/{address}/{unique_id}"


def generate_random_record():
    user_id = generate_user_id()
    product_count = random.randint(1, 10)  # Random number of products
    total_price = round(random.uniform(10.0, 500.0), 2)  # Random price between 10 and 500
    record_time = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())  # Current time in UTC

    return {
        "user_id": user_id,
        "product_count": product_count,
        "total_price": total_price,
        "time": record_time
    }


def send_to_kafka():
    while True:
        record = generate_random_record()
        producer.send(KAFKA_TOPIC, value=record)
        print(f"Sent record: {record}")
        time.sleep(1)  # Wait for 1 second before sending the next record


if __name__ == "__main__":
    try:
        send_to_kafka()
    except KeyboardInterrupt:
        print("Stopping the data generator.")
    finally:
        producer.close()
