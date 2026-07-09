from kafka.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token('ap-south-1')
        return token

token = MSKTokenProvider()

from kafka import KafkaConsumer
from kafka.coordinator.assignors.range import RangePartitionAssignor
import json, time

consumer = KafkaConsumer(
    '3aayaam-kafka-consumer-topic-2',
    bootstrap_servers='b-1-public.3aayaamkakfamskcluste.t1lcpw.c2.kafka.ap-south-1.amazonaws.com:9198,b-2-public.3aayaamkakfamskcluste.t1lcpw.c2.kafka.ap-south-1.amazonaws.com:9198',
    group_id='3aayaam-consumer-group-2',
    client_id='consumer-1',
    partition_assignment_strategy=[RangePartitionAssignor],
    max_poll_records=10,
    security_protocol='SASL_SSL',
    ssl_cafile='/Users/soumyadeepdey/DeepHDD/3aayaamFolder/3aayaamDataEnggV2/14.KafkaStreamingIngestion/AmazonRootCA1.pem',
    sasl_mechanism='OAUTHBEARER',
    sasl_oauth_token_provider=token,
    key_deserializer=lambda x: json.loads(x.decode('utf-8')),
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    # enable_auto_commit=True,
    # auto_commit_interval_ms=15000,
)

count=0
# for record in consumer:
#     print(record)
#     time.sleep(2)
#     count = count + 1

while True:
    record = consumer.poll(timeout_ms=500)
    print(record)
    time.sleep(2)