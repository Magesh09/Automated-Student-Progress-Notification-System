import json
import boto3
import csv
from io import StringIO

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client('sns')

def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']

    # Fetch the CSV file from S3
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    csv_content = response['Body'].read().decode('utf-8')

    # Parse CSV content
    csv_reader = csv.DictReader(StringIO(csv_content))
    table = dynamodb.Table('StudentProgress')

    # Process each row
    for row in csv_reader:
        studentID = row['studentID']
        name = row['Name']
        percentage = row['Percentage']
        email = row['email']

        # Store data in DynamoDB
        table.put_item(Item={
            'studentID': studentID,
            'Name': name,
            'Percentage': percentage,
            'email': email
        })

        # Publish a message to SNS
        sns_client.publish(
            TopicArn='arn:aws:sns:region:account-id:StudentProgressNotification',
            Subject='Student Progress Notification',
            Message=f'Dear {name},\n\nYour progress percentage is {percentage}%.'
        )

    return {
        'statusCode': 200,
        'body': json.dumps('Processing completed successfully.')
    }