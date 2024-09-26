from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pendulum

default_args = {
    'owner': 'khan',
    'start_date': datetime(2023, 9, 1),  # Ngày trong quá khứ
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def get_data():
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]

    return res


def format_data(res):
    data = {}
    location = res['location']

    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data


def stream_data():
    from kafka import KafkaProducer
    import json
    import time
    import logging

    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],
        max_block_ms=5000
    )
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60:
            break
        try:
            res = get_data()
            res = format_data(res)
            producer.send('users_created', json.dumps(res).encode('utf8'))
            logging.info(f"Data sent to Kafka: {res}")
        except Exception as e:
            logging.error(f"Error occurred while streaming data: {str(e)}", exc_info=True)
            continue


# def stream_data():
#     from kafka import KafkaProducer
#     import json
#     import time
#     import logging

#     producer = KafkaProducer(
#         bootstrap_servers=['localhost:9092'],
#         max_block_ms=5000
#     )

#     res = get_data()
#     res = format_data(res)
#     producer.send('users_created', json.dumps(res).encode('utf8'))


with DAG('user_automation', default_args=default_args, schedule=timedelta(minutes=1), catchup=False) as dag:
    streaming_task = PythonOperator(
        task_id='stream',
        python_callable=stream_data
    )

# stream_data()
