from kafka.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token('ap-south-1')
        return token

token = MSKTokenProvider()

from kafka import KafkaConsumer
import json, time

consumer = KafkaConsumer(
    '3aayaam-kafka-topic',
    bootstrap_servers='',
    group_id='3aayaam-consumer-group-1',
    max_poll_recors=10,
    security_protocol='SASL_SSL',
    ssl_cafile='/Users/soumyadeepdey/DeepHDD/3aayaamFolder/3aayaamDataEnggV2/14.KafkaStreamingIngestion/AmazonRootCA1.pem',
    sasl_mechanism='OAUTHBEARER',
    sasl_oauth_token_provider=token,
    key_deserializer=lambda x: json.loads(x.decode('utf-8')),
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
)

for record in consumer:
    print(record)
    time.sleep(5)