from kafka.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token('ap-south-1')
        return token

token = MSKTokenProvider()

import boto3, csv, time
from aws_schema_registry import SchemaRegistryClient
from aws_schema_registry.avro import AvroSchema
from aws_schema_registry.adapter.kafka import KafkaSerializer

glue_client = boto3.client('glue','ap-south-1')
glue_schema = glue_client.get_schema_version(
    SchemaVersionId='266b8f99-a6d7-41d3-9a75-e197836f56b9'
)
glue_schema = glue_schema['SchemaDefinition']
schema = AvroSchema(glue_schema)
# print(schema)

serializer_client = SchemaRegistryClient(glue_client,'3aayaam-kafka-schema-registry')
serializer=KafkaSerializer(serializer_client)


from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='b-1-public.3aayaamkafkacluster.by8xxo.c2.kafka.ap-south-1.amazonaws.com:9198,b-2-public.3aayaamkafkacluster.by8xxo.c2.kafka.ap-south-1.amazonaws.com:9198',
    security_protocol='SASL_SSL',
    ssl_cafile='/Users/soumyadeepdey/DeepHDD/3aayaamFolder/3aayaamDataEnggV2/14.KafkaStreamingIngestion/AmazonRootCA1.pem',
    sasl_mechanism='OAUTHBEARER',
    sasl_oauth_token_provider=token,
    key_serializer=serializer,
    value_serializer=serializer,
)

with open('kafka_dataset/kafka_dataset_1.csv', 'r') as ifile:
    records = csv.DictReader(ifile)
    for record in records:
        print(record)
        record['id'] = int(record['id'])
        producer.send('3aayaam-schreg-topic',value=(record,schema))
        time.sleep(2)

producer.flush()
