import requests
import boto3
import datetime
import time
from decimal import Decimal
from datetime import datetime, timezone

# ✅ **Yelp API Credentials (Replace immediately after use for security)**
YELP_API_KEY = "ENTER_YOUR_KEY"
YELP_API_HOST = "https://api.yelp.com/v3/businesses/search"

# ✅ **AWS DynamoDB Setup**
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
table = dynamodb.Table("yelp-restaurants")

# ✅ **List of cuisines to scrape**
CUISINES = ["Indian", "Italian", "Ethiopian", "American", "Mexican", "Japanese", "French", "Spanish", "Chinese"]
LOCATION = "Manhattan, NY" #Ran the script for Brooklyn as well
LIMIT = 50  # Yelp allows max 50 results per request
SORTING_OPTIONS = ["rating", "best_match", "review_count", "distance"]  # Changing sort order

def convert_float_to_decimal(value):
    """Convert float to Decimal to avoid DynamoDB TypeError"""
    if isinstance(value, float):
        return Decimal(str(value))  # Convert float to string first to maintain precision
    return value

def fetch_restaurants(cuisine):
    """Fetch available restaurants for a given cuisine using Yelp API, dynamically changing sorting if needed."""
    restaurants = {}
    for sort_by in SORTING_OPTIONS:  # Try different sorting methods
        print(f"📌 Fetching {cuisine} with sorting: {sort_by}")
        offset = 0
        total_fetched = 0

        while True:
            params = {
                "term": f"{cuisine} restaurants",
                "location": LOCATION,
                "limit": LIMIT,
                "offset": offset,
                "sort_by": sort_by
            }
            headers = {"Authorization": f"Bearer {YELP_API_KEY}"}

            try:
                response = requests.get(YELP_API_HOST, headers=headers, params=params)
                response.raise_for_status()  # Raise exception for HTTP errors
                data = response.json()

                businesses = data.get("businesses", [])
                num_results = len(businesses)

                if num_results == 0:
                    print(f"⚠️ Stopping early: No more restaurants found at offset {offset} for {cuisine} ({sort_by}).")
                    break  # Stop paginating if no more results

                for business in businesses:
                    if "id" not in business:
                        print("⚠️ Skipping entry without BusinessID")
                        continue  # Skip if no Business ID (needed for DynamoDB key)

                    # Store only unique business IDs across sorting methods
                    if business["id"] not in restaurants:
                        restaurants[business["id"]] = {
                            "business_id": business["id"],  # ✅ Updated key
                            "name": business.get("name", "Unknown"),
                            "address": " ".join(business.get("location", {}).get("display_address", ["Unknown"])),
                            "coordinates": {
                                "Latitude": convert_float_to_decimal(business.get("coordinates", {}).get("latitude", 0)),
                                "Longitude": convert_float_to_decimal(business.get("coordinates", {}).get("longitude", 0))
                            },
                            "numberOfReviews": convert_float_to_decimal(business.get("review_count", 0)),
                            "rating": convert_float_to_decimal(business.get("rating", 0)),
                            "zipCode": business.get("location", {}).get("zip_code", "Unknown"),
                            "cuisine": cuisine,
                            "sortingMethod": sort_by,  # Track sorting method used
                            "insertedAtTimestamp": datetime.now(timezone.utc).isoformat()
                        }
                        total_fetched += 1

                offset += LIMIT  # Move to next batch

                # ✅ **Stop early if we got fewer than 50 results (no more pages available)**
                if num_results < LIMIT:
                    print(f"⚠️ Stopping early: Yelp returned only {num_results} results at offset {offset - LIMIT} ({sort_by}).")
                    break  

                time.sleep(1)  # Prevent hitting API rate limits

            except requests.exceptions.RequestException as e:
                print(f"❌ Error fetching data from Yelp for {cuisine} at offset {offset} ({sort_by}): {e}")
                print(f"⚠️ Switching sorting method since {sort_by} failed at {offset}.")
                break  # Move to the next sorting method

        print(f"✅ Finished fetching {len(restaurants)} unique {cuisine} restaurants using {sort_by} sorting.\n")

    return list(restaurants.values())  # Convert dictionary back to a list

def store_in_dynamodb(data, cuisine):
    """Store restaurant data in DynamoDB."""
    if not data:
        print(f"⚠️ No data to store for {cuisine}. Skipping DynamoDB insert.")
        return

    with table.batch_writer() as batch:
        for item in data:
            try:
                batch.put_item(Item=item)
            except Exception as e:
                print(f"❌ Error storing item in DynamoDB: {e}")

if __name__ == "__main__":
    for cuisine in CUISINES:
        print(f"📡 Fetching data for {cuisine} cuisine...")
        restaurant_data = fetch_restaurants(cuisine)

        if restaurant_data:
            print(f"✅ Storing {len(restaurant_data)} {cuisine} restaurants in DynamoDB...")
            store_in_dynamodb(restaurant_data, cuisine)
            print(f"🎉 Done with {cuisine}!\n")
        else:
            print(f"⚠️ No data fetched for {cuisine}. Skipping...\n")

    print("✅ All restaurants successfully stored in DynamoDB!")
