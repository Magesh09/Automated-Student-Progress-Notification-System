import boto3
import json
import csv
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
ses = boto3.client('ses', region_name='us-east-1')  # Your SES region
sns = boto3.client('sns')
table = dynamodb.Table('student_scores')  # Your DynamoDB table name
sns_topic_arn = 'arn:aws:sns:us-east-1:007034119270:student-performance-reports'  # Your SNS topic ARN
SENDER_EMAIL = 'mageshnarayanaswamy5@gmail.com'  # Your verified SES email

def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    s3_client = boto3.client('s3')

    # Download the file from S3
    s3_client.download_file(bucket_name, file_key, '/tmp/temp_file.csv')

    students = []
    with open('/tmp/temp_file.csv', 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Process each row and insert into DynamoDB
            day = datetime.now().strftime('%d')
            month = datetime.now().strftime('%B')
            time = datetime.now().strftime('%H:%M:%S')
            table.put_item(
                Item={
                    'student_id': row['student_id'],
                    'name': row['name'],
                    'percentage': row['percentage'],
                    'email': row['email'],
                    'day': day,
                    'month': month,
                    'time': time
                }
            )
            students.append({
                'name': row['name'],
                'percentage': row['percentage'],
                'email': row['email']
            })

    # Publish to SNS for each student
    for student in students:
        subject = "Performance Report"
        body = f"""
        Hello {student['name']},

        We have processed your performance. Here are your performance details:
        
        Percentage: {student['percentage']}%
        
        If you have any questions or need further assistance, please let us know.

        Best regards,
        Your Team
        """

        message = {
            'default': body,
            'email': body
        }

        sns.publish(
            TopicArn=sns_topic_arn,
            Subject=subject,
            MessageStructure='json',
            Message=json.dumps(message)
        )

    return {
        'statusCode': 200,
        'body': json.dumps('File processed and emails sent successfully')
    }
