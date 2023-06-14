# The Cloud Functions for Firebase SDK to create Cloud Functions and set up triggers.
from firebase_functions import firestore_fn, https_fn

# The Firebase Admin SDK to access Cloud Firestore.
from firebase_admin import initialize_app, firestore
import google.cloud.firestore

import json

app = initialize_app()
db = firestore.client()

###
# HELPER FUNCTIONS
###

def pushAds(user_docs):
    total_users = len(user_docs)
    if total_users == 0:
        return 0
    
    for user_doc in user_docs:
        doc = user_doc.to_dict()
        print(f'{doc["email_address"]}')
        #print(f'{doc["device_registration_token"]}')

    return total_users

def emailAds(user_docs):
    total_users = len(user_docs)
    if total_users == 0:
        return 0
    
    for user_doc in user_docs:
        doc = user_doc.to_dict()
        print(f'{doc["email_address"]}')

    return total_users

def smsAds(user_docs):
    total_users = len(user_docs)
    if total_users == 0:
        return 0
    
    for user_doc in user_docs:
        doc = user_doc.to_dict()
        print(f'{doc["phone_number"]}')

    return total_users

###
# FIREBASE FUNCTIONS
###

# Listens for new documents to be created in /advertisements. Finds appropriate users to serve ads to.
@firestore_fn.on_document_created(document="advertisements/{pushId}")
def serve_new_ad(
    event: firestore_fn.Event[firestore_fn.DocumentSnapshot | None],
) -> None:

    if event.data is None:
        return

    try:
        audience = event.data.get("target_audience")
        categories = event.data.get("categories")
        target_count = event.data.get("target_user_count")
    except KeyError:
        # Not a valid targeted ad
        return
    
    users_ref = db.collection('users')
    user_query = users_ref.where('data_profile.interests', 'array-contains-any', categories)
    if audience["ethnicity"][0] != "Any":
        user_query = user_query.where('data_profile.ethnicity', 'in', audience["ethnicity"])
    if audience["gender"][0] != "Any":
        user_query = user_query.where('data_profile.gender', 'in', audience["gender"])
    if audience["home_ownership"][0] != "Any":
        user_query = user_query.where('data_profile.home_ownership', 'in', audience["home_ownership"])
    if audience["income"][0] != "Any":
        user_query = user_query.where('data_profile.income', 'in', audience["income"])
    if audience["location"][0] != "Any":
        user_query = user_query.where('data_profile.location', 'in', audience["location"])
    if audience["min_age"] != 18 and audience["max_age"] != 120:
        user_query = user_query.where('data_profile.age', '>=', audience["min_age"]).where('data_profile.age', "<=", audience["max_age"])
    if audience["politics"][0] != "Any":
        user_query = user_query.where('data_profile.politics', 'in', audience["politics"])
    if audience["relationship"][0] != "Any":
        user_query = user_query.where('data_profile.relationship', 'in', audience["relationship"])
    if audience["religion"][0] != "Any":
        user_query = user_query.where('data_profile.religion', 'in', audience["religion"])
    if audience["sexuality"][0] != "Any":
        user_query = user_query.where('data_profile.sexuality', 'in', audience["sexuality"])
    
    engaged_users = 0
    if audience["outreach"] == "push":
        user_query = user_query.where('data_profile.outreach.push.disabled', '==', 'false')
        pushAds(user_query.limit(target_count).stream())
    elif audience["outreach"] == "email":
        user_query = user_query.where('data_profile.outreach.email.disabled', '==', 'false')
        engaged_users = emailAds(user_query.limit(target_count).stream())
    elif audience["outreach"] == "sms":
        user_query = user_query.where('data_profile.outreach.sms.disabled', '==', 'false')
        engaged_users = smsAds(user_query.limit(target_count).stream())

    event.data.reference.update({"engaged_user_count": engaged_users})



# Create a new /advertisement document
@https_fn.on_request()
def create_new_ad(req: https_fn.Request):
    if req.data is None:
        return https_fn.Response("No payload was passed to the API, so no ad was created", status=400)
    try:
        data = json.loads(req.data)
        ad_data = {
            "action_title": data["action_title"],
            "categories": data["categories"],
            "description": data["description"],
            "engaged_user_count": data["engaged_user_count"],
            "media_location": data["media_location"],
            "media_type": data["media_type"],
            "name": data["name"],
            "outreach": data["outreach"],
            "target_audience": data["target_audience"],
            "target_user_count": data["target_user_count"],
            "target_url": data["target_url"]
        }
    except KeyError:
        # Throwing an HttpsError so that the client gets the error details.
        raise https_fn.HttpsError(
            code=https_fn.FunctionsErrorCode.INVALID_ARGUMENT,
            message=(
                """The function must be called with the following payload:
                "action_title": String,
                "categories": String[]],
                "description": String,
                "engaged_user_count": Integer,
                "media_location": String || String[],
                "media_type": String,
                "name": String,
                "outreach": String,
                "target_audience": Map,
                "target_user_count": Integer,
                "target_url": String 
                """
            ),
        )
    try:
        create_time, ad_ref = db.collection('advertisements').add(ad_data)
        return https_fn.Response(f'Created new /advertisement document with id: {ad_ref.id}', status=200)
    except:
        return https_fn.Response(f'Advertisement couldn\'t be created with the provided request payload: {req.data}', status=400)


# Update /advertisements document's engaged_user_count
# @https_fn.on_request()
# def addmessage(req: https_fn.Request) -> https_fn.Response:
#     """Take the text parameter passed to this HTTP endpoint and insert it into
#     a new document in the messages collection."""
#     # Grab the text parameter.
#     original = req.args.get("text")
#     if original is None:
#         return https_fn.Response("No text parameter provided", status=400)

#     firestore_client: google.cloud.firestore.Client = firestore.client()

#     # Push the new message into Cloud Firestore using the Firebase Admin SDK.
#     _, doc_ref = firestore_client.collection("messages").add(
#         {"original": original}
#     )

#     # Send back a message that we've successfully written the message
#     return https_fn.Response(f"Message with ID {doc_ref.id} added.")


# main.py
