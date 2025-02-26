import json
import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from botocore.exceptions import BotoCoreError, NoCredentialsError
import time

# ✅ AWS Configuration
REGION = "us-east-1"
SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/863518423950/Q1"
SES_SENDER_EMAIL = "ronittushir.dev@gmail.com"  # Must be verified in SES

# ✅ OpenSearch Configuration (Using Basic Authentication)
OPENSEARCH_HOST = "search-restaurant-search-v2-xyaxfxfe7jspzdybcadeaciwvu.us-east-1.es.amazonaws.com"
INDEX_NAME = "restaurants"
OPENSEARCH_USERNAME = "ENTER_YOUR_OPENSEARCH_USERNAME"
OPENSEARCH_PASSWORD = "ENTER_YOUR_OPENSEARCH_PASSWORD"

# ✅ DynamoDB Table Configuration
DYNAMODB_TABLE = "yelp-restaurants"

# ✅ AWS Clients
sqs = boto3.client("sqs", region_name=REGION)
ses = boto3.client("ses", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table(DYNAMODB_TABLE)


def fetch_sqs_messages():
    """Retrieve messages from SQS queue."""
    try:
        response = sqs.receive_message(
            QueueUrl=SQS_QUEUE_URL,
            MaxNumberOfMessages=5,  # Fetch up to 5 messages per invocation
            MessageAttributeNames=["All"],  # Ensure all attributes are included
            VisibilityTimeout=30,  # Time before a message becomes visible again
            WaitTimeSeconds=0  # Set to 0 for immediate response
        )

        if "Messages" in response:
            return response["Messages"]
        else:
            print("⚠️ No messages in SQS queue.")
            return []
    
    except Exception as e:
        print(f"❌ Error fetching messages from SQS: {e}")
        return []


def query_opensearch(cuisine):
    """Query OpenSearch using username/password authentication."""
    query = {
        "size": 5,
        "query": {
            "function_score": {
                "query": {
                    "match": {
                        "cuisine": cuisine
                    }
                },
                "random_score": {"seed": int(time.time())},  # Randomize results each time
                "boost_mode": "sum"
            }
        }
    }

    try:
        client = OpenSearch(
            hosts=[{"host": OPENSEARCH_HOST, "port": 443}],
            http_auth=(OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD),  # ✅ Basic Auth
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )

        response = client.search(index=INDEX_NAME, body=query)
        hits = response["hits"]["hits"]

        return [hit["_source"]["business_id"] for hit in hits] if hits else []
    
    except Exception as e:
        print(f"❌ Error querying OpenSearch: {e}")
        return []

def get_dynamo_details(business_ids):
    """Retrieve restaurant details from DynamoDB using business IDs."""
    details = []
    for business_id in business_ids:
        try:
            response = table.get_item(Key={"business_id": business_id})
            if "Item" in response:
                item = response["Item"]
                details.append(f"{item['name']} at {item['address']}")
        except Exception as e:
            print(f"❌ Error fetching from DynamoDB: {e}")
    return details

def send_email(recipient, recommendations, cuisine, location, time, num_people):
    """Send restaurant recommendations via AWS SES."""
    subject = f"🍽️ Dining Suggestions for {cuisine} Cuisine in {location}"

    body = f"""
    <html>
    <body>
        <h3>Hello!</h3>
        <p>Here are some <strong>{cuisine}</strong> restaurant suggestions for <strong>{num_people} people</strong> at <strong>{time}</strong> in <strong>{location}</strong>:</p>
        <ul>
            {''.join([f"<li>{i+1}. {rec}</li>" for i, rec in enumerate(recommendations)])}
        </ul>
        <p>Enjoy your meal! 🍽️</p>
    </body>
    </html>
    """

    try:
        response = ses.send_email(
            Source=SES_SENDER_EMAIL,
            Destination={"ToAddresses": [recipient]},
            Message={
                "Subject": {"Data": subject},
                "Body": {"Html": {"Data": body}}
            }
        )
        print(f"✅ Email sent successfully to {recipient} and response confirming : {response}.")
        return response
    except Exception as e:
        print(f"❌ Error sending email: {e}")
        return None

def lambda_handler(event, context):
    """Main Lambda function to fetch and process SQS messages."""
    print("📩 Fetching messages from SQS...")
    
    messages = fetch_sqs_messages()
    
    if not messages:
        return {"statusCode": 200, "body": "⚠️ No new SQS messages to process."}

    for message in messages:
        try:
            attributes = message["MessageAttributes"]
            
            cuisine = attributes["Cuisine"]["StringValue"]
            location = attributes["Location"]["StringValue"]
            dining_time = attributes["Time"]["StringValue"]
            num_people = attributes["People"]["StringValue"]
            email = attributes["Email"]["StringValue"]
            
            print(f"✅ Extracted: {cuisine}, {location}, {dining_time}, {num_people}, {email}")

            # Query OpenSearch
            business_ids = query_opensearch(cuisine)
            print(f"✅ OpenSearch returned: {business_ids}")

            # Fetch restaurant details
            restaurant_details = get_dynamo_details(business_ids)
            print(f"✅ Retrieved restaurant details: {restaurant_details}")

            if restaurant_details:
                send_email(email, restaurant_details, cuisine, location, dining_time, num_people)
                print(f"📧 Email sent to {email}")

            # ✅ Delete message after successful processing
            sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=message["ReceiptHandle"])
            print(f"✅ Deleted message from SQS queue")

        except KeyError as e:
            print(f"❌ Missing attribute in message: {e}")
            continue  # Skip processing this message
    
    return {"statusCode": 200, "body": "✅ Processed SQS messages"}