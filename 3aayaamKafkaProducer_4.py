from kafka.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token('ap-south-1')
        return token

token = MSKTokenProvider()

from kafka import KafkaProducer
import json, time

producer = KafkaProducer(
    bootstrap_servers='b-3-public.3aayaammskkafkacluste.nrosbm.c2.kafka.ap-south-1.amazonaws.com:9198,b-1-public.3aayaammskkafkacluste.nrosbm.c2.kafka.ap-south-1.amazonaws.com:9198,b-2-public.3aayaammskkafkacluste.nrosbm.c2.kafka.ap-south-1.amazonaws.com:9198',
    client_id='3aayaam-kafka-producer',
    batch_size=49152,
    security_protocol='SASL_SSL',
    ssl_cafile='/Users/soumyadeepdey/DeepHDD/3aayaamFolder/3aayaamDataEnggV2/14.KafkaStreamingIngestion/AmazonRootCA1.pem',
    sasl_mechanism='OAUTHBEARER',
    sasl_oauth_token_provider=token,
    key_serializer=lambda x: json.dumps(x).encode('utf-8'),
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    # acks=0,
    acks='all',
    enable_idempotence=True,
)


with open('kafka_dataset/kafka_dataset_4.csv', 'r') as input_file:
    records = input_file.readlines()
    for record in records:
        id, start_date, end_date = record.split(',')

        producer.send('3aayaam-kafka-topic', key=id, value=record)
        time.sleep(5)

producer.flush()






