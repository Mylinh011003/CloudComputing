import csv
from confluent_kafka import Producer
import pandas as pd
import time
# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # Địa chỉ Kafka broker
    'client.id': 'csv-producer',
    'queue.buffering.max.messages': 1000000,   # Tăng số lượng message tối đa trong buffer
    'queue.buffering.max.kbytes': 10240,       # Tăng dung lượng buffer (10MB)
    'queue.buffering.max.ms': 1000,
}

# Khởi tạo Producer
producer = Producer(conf)

# Hàm gửi message lên Kafka
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Đọc dữ liệu từ file CSV
df = pd.read_csv('./Data/transaction_data/transaction_data.csv')  # Đọc dữ liệu từ file CSV

# Lặp qua từng dòng dữ liệu và gửi lên Kafka
topic = 'transaction_test'
for index, row in df.iterrows():
    message = row.to_json()  # Chuyển dòng thành chuỗi JSON
    producer.produce(topic, value=message, callback=delivery_report)
    time.sleep(2)

# Chờ tất cả message được gửi đi
# producer.flush()