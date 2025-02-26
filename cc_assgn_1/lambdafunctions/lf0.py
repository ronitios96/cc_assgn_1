import json
import boto3
import uuid

# Initialize Lex client
client = boto3.client('lexv2-runtime')
REGION = "us-east-1"
dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table("user-preferences")

# Constants for Bot ID and Alias
BOT_ID = 'WTZULWDAIL'          # Replace with your Lex bot ID
BOT_ALIAS_ID = 'QVVGXKUXCC'    # Replace with your Lex bot alias
LOCALE_ID = 'en_US'            # Set locale

def get_sessionId(event):
    """Generates a session ID dynamically based on user data."""
    return event.get('sessionId') or str(uuid.uuid4())

def lambda_handler(event, context):
    try:
        # Extract the user message safely
        msg_from_user = event.get('messages', [{}])[0].get('unstructured', {}).get('text', '')

        if not msg_from_user:
            return generate_response("Sorry, I didn't understand that. Please try again.")
    
        session_id = get_sessionId(event)

         # **🔹 Check if user has previous preferences stored**
        previous_search = get_user_preferences(session_id)
        print(f"previous_search: {previous_search}")
        print(f"session_id: {session_id}")
        # Send the user message to Lex and get a response
        response = client.recognize_text(
            botId=BOT_ID,
            botAliasId=BOT_ALIAS_ID,
            localeId=LOCALE_ID,
            sessionId=session_id,
            text=msg_from_user
        )

        intentFromLex = response.get('sessionState', {}).get('intent', {}).get('name', 'DefaultFallbackIntent')
        if (intentFromLex == "DiningSuggestionsIntent") and previous_search:
            if msg_from_user in ["yes", "no"]:
                return handle_yes_no_response(session_id, msg_from_user, previous_search)
            # Ask the user if they want to continue with stored preferences
            return generate_response(f"We have your last search for {previous_search['cuisine']} in {previous_search['location']}. Would you like to continue? (yes/no)") 
        
        # Extract Lex response message safely
        msg_from_lex = response.get('messages', [])
        if not msg_from_lex:
            return generate_response("I'm not sure how to respond to that right now.")

        # Build the response to send back
        return generate_response(msg_from_lex[0].get('content', "I couldn't understand that."))
    
    except Exception as e:
        print(f"Error: {e}")
        return generate_response("There was an error processing your request. Please try again later.")

# Helper function to structure response properly
def generate_response(message_text):
    return {
        'statusCode': 200,
        'messages': [
            {
                'type': 'unstructured',
                'unstructured': {
                    'text': message_text
                }
            }
        ]
    }

# **🔹 Check Previous Preferences in DynamoDB**
def get_user_preferences(session_id):
    try:
        response = table.get_item(Key={"sessionId": session_id})
        print(f"response: {response}")
        return response.get("Item")
    except Exception as e:
        print(f"Error fetching preferences: {e}")
        return "Error fetching preferences"

def handle_yes_no_response(session_id, response, previous_search):
    if response == "yes":
        return continue_with_stored_preferences(session_id, previous_search)
    elif response == "no":
        delete_user_preferences(session_id)
        return generate_response("Preferences cleared. Let's start fresh. How can I help you today?")

def delete_user_preferences(session_id):
    try:
        table.delete_item(Key={"sessionId": session_id})
        print("✅ Preferences deleted")
    except Exception as e:
        print(f"Error deleting preferences: {e}")

# **🔹 Continue with Stored Preferences by Triggering DiningSuggestionsIntent**
def continue_with_stored_preferences(session_id, previous_search):
    try:
        lex_response = client.recognize_text(
            botId=BOT_ID,
            botAliasId=BOT_ALIAS_ID,
            localeId=LOCALE_ID,
            sessionId=session_id,
            text="What time do you prefer?",  # Provide a valid message
            sessionState={
                "dialogAction": {
                    "type": "ElicitSlot",
                    "slotToElicit": "Time"  # Ask for the next missing slot (adjust as needed)
                },
                "intent": {
                    "name": "DiningSuggestionsIntent",
                    "slots": {
                        "Cuisine": {
                            "shape": "Scalar",
                            "value": {
                                "originalValue": previous_search["cuisine"],
                                "resolvedValues": [previous_search["cuisine"]],
                                "interpretedValue": previous_search["cuisine"]
                            }
                        },
                        "Location": {
                            "shape": "Scalar",
                            "value": {
                                "originalValue": previous_search["location"],
                                "resolvedValues": [previous_search["location"]],
                                "interpretedValue": previous_search["location"]
                            }
                        }
                    },
                    "state": "InProgress"
                }
            }
        )

        msg_from_lex = lex_response.get('messages', [])

        if msg_from_lex:
            table.delete_item(Key={"sessionId": session_id})
            print("✅ Preferences consumed so they are deleted")
            return generate_response(msg_from_lex[0].get('content', "I'm processing your request."))
        else:
            return generate_response("Something went wrong while processing your request.")

    except Exception as e:
        print(f"Error in continuing stored preferences: {e}")
        return generate_response("Error while continuing your request. Please try again.")
