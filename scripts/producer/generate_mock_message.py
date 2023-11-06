import time
from kafka import KafkaProducer
import pandas as pd
import os

def send_data_from_csv(topic=None, csv_path=None):
    # connect to kafka
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    dataframe = pd.read_csv(csv_path)

    # loop each row in dataframe, dump to json and send to kafka
    for i in dataframe.index:
        data = dataframe.loc[i].to_json(orient='index', indent=1)
        producer.send(topic, data.encode('utf-8'))

        if i % 10 == 0:
            time.sleep(0.1)

    # block until all async messages are sent
    producer.flush()


if __name__ == '__main__':
    this_script_dir = os.path.dirname(os.path.realpath(__file__))
    mock_msg_dir_path_string = os.path.join(
        os.path.dirname(
            this_script_dir
        ),
        'producer',
        'order_book_mockup.csv',
    )
    send_data_from_csv("technical_assessment", mock_msg_dir_path_string)
