import boto3, os


def write_file_to_s3(s3_client, output_bucket, output_object):
    s3_client.upload_file('/tmp/processed_emp_data.csv', output_bucket, output_object)


def process_data(s3_bucket, s3_object_key, output_bucket, output_object):
    s3_client = boto3.client('s3', 'ap-south-1')

    s3_client.download_file(s3_bucket, s3_object_key, '/tmp/emp_data.csv')

    with open('/tmp/emp_data.csv', 'r') as f:
        lines = f.readlines()

    with open('/tmp/processed_emp_data.csv', 'w') as of:
        for line in lines:
            emp_id, first_name, last_name, email, gender, ip_address = line.split(',')
            record = str(emp_id) + ',' + str(email)
            of.write(record + '\n')

    write_file_to_s3(s3_client, output_bucket, output_object)


def s3_handler(event, context):
    output_bucket = os.environ['BUCKET_NAME']
    output_object = os.environ['OBJECT_NAME']
    s3_bucket = event['Records'][0]['s3']['bucket']['name']
    s3_object_key = event['Records'][0]['s3']['object']['key']
    process_data(s3_bucket, s3_object_key, output_bucket, output_object)