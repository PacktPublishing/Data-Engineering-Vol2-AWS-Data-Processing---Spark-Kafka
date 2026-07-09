from kafka.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token('ap-south-1')
        return token

token = MSKTokenProvider()

from kafka import KafkaConsumer
import json, time
from kafka.coordinator.assignors.range import RangePartitionAssignor

consumer = KafkaConsumer(
    '3aayaam-kafka-topic-0',
    bootstrap_servers = 'b-2-public.3aayaamkafkamskcluste.m304fg.c2.kafka.ap-south-1.amazonaws.com:9198,b-1-public.3aayaamkafkamskcluste.m304fg.c2.kafka.ap-south-1.amazonaws.com:9198',
    group_id='consumer-group-1',
    client_id = 'consumer-1',
    partition_assignment_strategy=[RangePartitionAssignor],
    max_poll_records=10,
    security_protocol='SASL_SSL',
    ssl_cafile='/Users/soumyadeepdey/DeepHDD/3aayaamFolder/3aayaamDataEnggV2/14.KafkaStreamingIngestion/AmazonRootCA1.pem',
    sasl_mechanism='OAUTHBEARER',
    sasl_oauth_token_provider=token,
    key_deserializer=lambda x: json.loads(x.decode('utf-8')),
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    enable_auto_commit=False,
    auto_offset_reset = 'latest',
)


for record in consumer:
    print(record)
    time.sleep(2)

# starting key = 1
