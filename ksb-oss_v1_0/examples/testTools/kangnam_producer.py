import sys
import csv
import json

from time import sleep
from kafka import KafkaProducer # pip install kafka-python


def main():
    csv_path = sys.argv[1]
    kafka_servers = sys.argv[2]
    topic = sys.argv[3]
    period = float(sys.argv[4])
    print(csv_path, kafka_servers, topic, period)

    producer = KafkaProducer(bootstrap_servers=kafka_servers)

    loop = True
    while loop:
        try:
            produce(csv_path, producer, topic, period)
        except KeyboardInterrupt:
            loop = False
    print('terminated')


def produce(csv_path, producer, topic, period):
    with open(csv_path, mode='r') as f:
        reader = csv.reader(f)
        next(reader, None) # skip header line.
        for row in reader:
            js = json.dumps({
                'PRCS_DATE': row[0],
                'LINK_ID': row[1],
                'PRCS_SPD': float(row[2])
            })
            producer.send(topic, js.encode())
            print(js)
            sleep(period)


if __name__ == '__main__':
    main()
