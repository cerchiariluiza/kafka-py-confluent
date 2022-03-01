import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
import time
import random
import numpy as np
from confluent_kafka import KafkaError, Producer

KAFKA_TOPIC_NAME_CONS = "top"
p = Producer({'bootstrap.servers': 'pkc-2396y.us-east-1.aws.confluent.cloud:9092','security.protocol':'SASL_SSL', 'sasl.mechanisms':'PLAIN','sasl.username':'', 'sasl.password':'})



filepath = "IRIS.csv"

flower_df = pd.read_csv(filepath)

flower_df['order_id'] = np.arange(len(flower_df))


flower_list = flower_df.to_dict(orient="records")


message_list = []
message = None
for message in flower_list:

    message_fields_value_list = []
            

    message_fields_value_list.append(message["sepal_length"])
    message_fields_value_list.append(message["sepal_width"])
    message_fields_value_list.append(message["petal_length"])
    message_fields_value_list.append(message["petal_width"])


    message = ','.join(str(v) for v in message_fields_value_list)
    print("Message Type: ", type(message))
    print("Message: ", message)
    p.produce(KAFKA_TOPIC_NAME_CONS, message)
    time.sleep(1)

    p.flush()
    print("Kafka Producer Application Completed. ")

