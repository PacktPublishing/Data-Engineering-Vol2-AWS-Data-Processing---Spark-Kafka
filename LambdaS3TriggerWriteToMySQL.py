import boto3, os, pymysql

def write_file_to_s3(s3_client, output_bucket, output_object):
    s3_client.upload_file('/tmp/processed_emp_data.csv', output_bucket, output_object)


def write_to_mysql(mysql_host,mysql_user,mysql_pwd,mysql_db,s3_bucket, s3_object_key):
    s3_client = boto3.client('s3', 'ap-south-1')
    s3_client.download_file(s3_bucket, s3_object_key, '/tmp/emp_data.csv')

    conn = pymysql.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_pwd,
        database=mysql_db
    )
    cur = conn.cursor()

    with open('/tmp/emp_data.csv', 'r') as f:
        lines = f.readlines()

    for line in lines:
        emp_id, first_name, last_name, email, gender, ip_address = line.split(',')
        cur.execute("INSERT INTO emp VALUES (%s, %s)",(emp_id,email))
        conn.commit()


def process_data(s3_bucket, s3_object_key, output_bucket, output_object, database_flag):
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
    database_flag = os.environ['DB_FLAG']
    s3_bucket = event['Records'][0]['s3']['bucket']['name']
    s3_object_key = event['Records'][0]['s3']['object']['key']

    if database_flag == 'Y':
        mysql_host = os.environ['DBHOST']
        mysql_user = os.environ['DBUSER']
        mysql_pwd = os.environ['DBPWD']
        mysql_db = os.environ['DBNAME']
        write_to_mysql(mysql_host,mysql_user,mysql_pwd,mysql_db,s3_bucket, s3_object_key)
    else:
        output_bucket = os.environ['BUCKET_NAME']
        output_object = os.environ['OBJECT_NAME']
        process_data(s3_bucket, s3_object_key, output_bucket, output_object, database_flag)