import json
import re
import dateutil.parser
import boto3
import logging
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import dateutil.parser

# ✅ AWS Configuration
REGION = "us-east-1"
dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table("user-preferences")
sqs = boto3.client("sqs")
QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/863518423950/Q1"

# ✅ Logging Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# **🔹 Store user preferences in DynamoDB**
def store_user_preferences(session_id, cuisine, location):
    try:
        table.put_item(
            Item={
                "sessionId": session_id,
                "cuisine": cuisine,
                "location": location,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        print("✅ Preferences saved successfully")
    except Exception as e:
        print(f"Error saving preferences: {e}")

# 🔹 **Validation Functions**
def is_valid_location(location):
    valid_locations = ["brooklyn", "manhattan"]
    return location and location.lower() in valid_locations

def is_valid_cuisine(cuisine):
    valid_cuisines = ["indian", "italian", "ethiopian", "american", "mexican", "japanese", "french", "spanish", "chinese"]
    return cuisine and cuisine.lower() in valid_cuisines

def is_valid_email(email):
    regex = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
    return email and bool(re.match(regex, email))

def is_valid_people_count(num_people):
    try:
        return num_people and 1 <= int(num_people) <= 10
    except (ValueError, TypeError):
        return False

def is_valid_time(time_str):
    """Ensures the provided time is in the future by interpreting it in the context of today."""
    try:
        # Parse the time assuming today's date
        now = datetime.now()
        parsed_time = dateutil.parser.parse(time_str, fuzzy=True)

        # If the parsed time is before now, check if it should be scheduled for tomorrow
        if parsed_time < now:
            # If the time format doesn't specify AM/PM explicitly, assume PM if it's ambiguous
            if "am" not in time_str.lower() and "pm" not in time_str.lower():
                parsed_time += timedelta(hours=12)  # Move to PM if needed

            # If still in the past, assume the user meant the next day
            if parsed_time < now:
                parsed_time += timedelta(days=1)

        return parsed_time > now  # Ensure the final computed time is still in the future

    except (ValueError, TypeError):
        return False

# 🔹 **Validation Handler**
def validate_parameters(time, cuisine, location, num_people, email):
    """Checks each slot for validity and returns appropriate response."""
    validations = [
        (location, is_valid_location, "Location", "Where would you like to eat? Brooklyn or Manhattan?", 
         "We do not have restaurants there. Please choose from Manhattan or Brooklyn."),
        (cuisine, is_valid_cuisine, "Cuisine", "What type of cuisine do you prefer?",
         f"We do not have any restaurant that serves {cuisine}. Would you like a different cuisine?"),
        (time, is_valid_time, "Time", "What time do you prefer?",
         "Please enter a valid time in the future."),
        (num_people, is_valid_people_count, "People", "How many people (including you) are going?",
         "Please enter a valid number of people (between 1 and 10)."),
        (email, is_valid_email, "Email", "Please share your email address.",
         "Please enter a valid email address.")
    ]
    
    for value, validator, slot, empty_msg, invalid_msg in validations:
        if not value:
            return build_validation_result(False, slot, empty_msg)
        if not validator(value):
            return build_validation_result(False, slot, invalid_msg)
    
    return build_validation_result(True, None, None)

# 🔹 **Helper Functions**
def build_validation_result(is_valid, violated_slot, message):
    """Constructs a validation response."""
    return {
        "isValid": is_valid,
        "violatedSlot": violated_slot,
        "message": {"contentType": "PlainText", "content": message} if message else None
    }

def elicit_slot(event, slot_name, prompt):
    """Asks Lex to re-prompt the user for a specific slot."""
    return {
        "sessionState": {
            "dialogAction": {
                "type": "ElicitSlot",
                "slotToElicit": slot_name
            },
            "intent": {
                "name": event["sessionState"]["intent"]["name"],
                "slots": event["sessionState"]["intent"].get("slots", {}),  # ✅ Safe extraction
                "state": "InProgress"
            }
        },
        "messages": [{"contentType": "PlainText", "content": prompt}]
    }

def push_to_sqs(msg_body):
    """Sends user request data to SQS for processing."""
    try:
        response = sqs.send_message(
            QueueUrl=QUEUE_URL,
            DelaySeconds=0,
            MessageAttributes={
                "Cuisine": {"DataType": "String", "StringValue": msg_body["Cuisine"]},
                "Location": {"DataType": "String", "StringValue": msg_body["Location"]},
                "Email": {"DataType": "String", "StringValue": msg_body["Email"]},
                "Time": {"DataType": "String", "StringValue": msg_body["Time"]},
                "People": {"DataType": "Number", "StringValue": str(msg_body["People"])},
            },
            MessageBody="Information about the diner"
        )
        logger.info(f"✅ SQS Message Sent: {response}")
        store_user_preferences(msg_body["sessionId"], msg_body["Cuisine"], msg_body["Location"])
        return response
    except ClientError as e:
        logger.error(f"❌ Error sending message to SQS: {e}")
        return None

# 🔹 **Lex Intent Handlers**
def handle_dining_suggestions(event):
    """Handles DiningSuggestionsIntent by validating inputs and sending to SQS."""
    slots = event["sessionState"]["intent"].get("slots", {})  # ✅ Safe extraction
    print("🔹 Slots:", slots)
    # Extract slots safely
    def get_slot_value(slot_name):
        if not slots or not slots.get(slot_name) :
            logger.warning("🚨 Slots object is missing! Event Payload:", event)
            return ""  # Return empty string instead of crashing
        return slots.get(slot_name, {}).get("value", {}).get("originalValue", "")

    location, cuisine, time, num_people, email = (
        get_slot_value("Location"),
        get_slot_value("Cuisine"),
        get_slot_value("Time"),
        get_slot_value("People"),
        get_slot_value("Email"),
    )

    # Validate user inputs
    validation_result = validate_parameters(time, cuisine, location, num_people, email)
    if not validation_result["isValid"]:
        return elicit_slot(event, validation_result["violatedSlot"], validation_result["message"]["content"])
    # Send data to SQS
    slot_dict = {"Time": time, "Cuisine": cuisine, "Location": location, "People": num_people, "Email": email, "sessionId": event["sessionId"]}
    if not push_to_sqs(slot_dict):
        return generate_response("Sorry, we couldn't process your request at this time. Please try again later.")

    response_message = f"Thank you! We'll send you {cuisine} food suggestions in {location} for {num_people} people at {time} on {email}."
    return generate_response(response_message, "DiningSuggestionsIntent")

def handle_greeting_intent(event):
    """Handles GreetingIntent."""
    return generate_response("Hello! Welcome! How can I be of assistance today?", "GreetingIntent")

def handle_thank_you_intent(event):
    """Handles ThankYouIntent."""
    return generate_response("You're welcome! Let me know if you need anything else.", "ThankYouIntent")

def handle_fallback_intent(event):
    """Handles cases where Lex doesn't understand the user's request."""
    return generate_response("Sorry, I didn't quite get that. Could you please rephrase?", "FallbackIntent")

def generate_response(message, intent_name=""):
    """Constructs a Lex response message."""
    return {
        "sessionState": {
            "dialogAction": {"type": "Close"},
            "intent": {"name": intent_name, "state": "Fulfilled"} if intent_name else {}
        },
        "messages": [{"contentType": "PlainText", "content": message}]
    }

# 🔹 **Main Lambda Function**
def lambda_handler(event, context):
    """Main Lex Lambda function that routes intent requests."""
    intent_name = event["sessionState"]["intent"]["name"]
    print("event", event)
    handlers = {
        "DiningSuggestionsIntent": handle_dining_suggestions,
        "GreetingIntent": handle_greeting_intent,
        "ThankYouIntent": handle_thank_you_intent,
        "FallbackIntent": handle_fallback_intent,
    }

    return handlers.get(intent_name, handle_fallback_intent)(event)  # ✅ SAFE default handler
