import requests
import boto3

# ✅ OpenSearch Configuration (Replace with your actual OpenSearch endpoint)
OPENSEARCH_URL = "https://search-restaurant-search-v2-xyaxfxfe7jspzdybcadeaciwvu.us-east-1.es.amazonaws.com"
INDEX_NAME = "restaurants"

# ✅ Basic Authentication Credentials (Master User)
OPENSEARCH_USER = "ENTER_YOUR_USER"
OPENSEARCH_PASS = "ENTER_YOUR_PASSWORD"

# ✅ DynamoDB Configuration
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")  # Update if needed
table = dynamodb.Table("yelp-restaurants")  # Ensure this matches your actual table name

def get_dynamo_data():
    """Fetch all restaurant data from DynamoDB"""
    response = table.scan()
    return response["Items"]

def store_in_opensearch(data):
    """Store each restaurant in OpenSearch"""
    headers = {"Content-Type": "application/json"}
    auth = (OPENSEARCH_USER, OPENSEARCH_PASS)  # ✅ Use Basic Auth

    for item in data:
        if "business_id" not in item:
            print(f"⚠️ Skipping entry without business_id: {item}")
            continue

        doc = {
            "business_id": item["business_id"],
            "cuisine": item.get("cuisine", "unknown")
        }

        # ✅ Correct OpenSearch URL (Using Basic Authentication)
        opensearch_url = f"{OPENSEARCH_URL}/{INDEX_NAME}/_doc/{doc['business_id']}"

        response = requests.put(opensearch_url, json=doc, headers=headers, auth=auth)

        if response.status_code in [200, 201]:
            print(f"✅ Added {doc['business_id']} to OpenSearch")
        else:
            print(f"❌ Error adding {doc['business_id']} to OpenSearch: {response.text}")

if __name__ == "__main__":
    print("📡 Fetching data from DynamoDB...")
    data = get_dynamo_data()
    print(f"✅ Fetched {len(data)} records.")

    print("🚀 Storing data in OpenSearch...")
    store_in_opensearch(data)
    print("🎉 Done!")
