import boto3


def process_data(s3_bucket, s3_object_key):
    s3_client = boto3.client('s3', 'ap-south-1')

    s3_client.download_file(s3_bucket, s3_object_key, '/tmp/emp_data.csv')

    with open('/tmp/emp_data.csv', 'r') as f:
        lines = f.readlines()
        for line in lines:
            emp_id, first_name, last_name, email, gender, ip_address = line.split(',')
            print(emp_id, email)


def s3_handler(event, context):
    s3_bucket = event['Records'][0]['s3']['bucket']['name']
    s3_object_key = event['Records'][0]['s3']['object']['key']
    process_data(s3_bucket, s3_object_key)