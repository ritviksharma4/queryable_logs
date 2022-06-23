from pymongo import MongoClient
from datetime import datetime
import urllib 

"""
    - Split the string of form dd-mm-yyyy to extract year, month and day
    - Split the string of form hh:mm:ss to extract hour, minutes and seconds
"""
def extractDateTime(date, time):
    date_terms = date.split("-")
    year, month, day = int(date_terms[2]), int(date_terms[1]), int(date_terms[0])
    time_terms = time.split(":")
    hour, minutes, seconds = int(time_terms[0]), int(time_terms[1]), int(time_terms[2])
    return year, month, day, hour, minutes, seconds


"""
    - Convert date of form datetime(yyyy, mm, dd, HH, MM, SS)
    - Create a dictionary/JSON with key-value pairs which is then added to MongoDB
"""
def insertOneDoc(date, time, membership_number, session_id, miniapp_name, main_action, middle_action, end_action):
    year, month, day, hour, minutes, seconds = extractDateTime(date, time)
    date_time = datetime(year, month, day, hour, minutes, seconds)
    entry = createDictionary(date_time, membership_number, session_id, miniapp_name, main_action, middle_action, end_action)
    mongo_collection.insert_one(entry)


"""
    - Returns a JSON object containing each parameter and it's value as a key-value pair.
"""
def createDictionary(date_time, membership_number, session_id, miniapp_name, main_action, middle_action, end_action):
    entry = {
        "Date_Time" : date_time,
        "Membership_Number" : membership_number,
        "Session_ID" : session_id,
        "Mini_App_Name" : miniapp_name,
        "Main_Action" : main_action,
        "Middle_Action" : middle_action,
        "End_Action" : end_action
    }
    return entry

def extractDate():
    pass

"""
    - Extract all entries from MongoDB
"""
def fetchAllEntries():
    entries = list(mongo_collection.find())
    return entries

"""
    - If a particular customer's details are required, match by Membership_Number
    - If a particular customer's logs are required for a specific date, match by Membership_Number and date. (Time-Frame?)
    - If a particular session's details are required, match by Membership_Number and SessionID
"""
def findParticularCustomerJourneys(membership_number, session_id = "NA"):
    customer_entries = []
    if session_id == "NA":
        customer_entries.append(mongo_collection.find({"Membership_Number" : membership_number}).sort("Date_Time", 1))
    else:
        customer_entries.append(mongo_collection.find({"Membership_Number" : membership_number, "Session_ID" : session_id}).sort("Date_Time", 1))
    return customer_entries



"""
    - Driver function
"""
if __name__ == '__main__':
    mongo_URI = "mongodb+srv://ritviksharma4:" + urllib.parse.quote("mongo!@#123") + "@cluster0.5q21c.mongodb.net/?retryWrites=true&w=majority"
    client = MongoClient(mongo_URI)
    client_logs_db = client.get_database("client_logs")
    mongo_collection = client_logs_db.banking_logs

    # Sample Insertion
    date = "26-05-2022"
    time = "15:58:12"
    membership_number = "2"
    session_id = "102"
    miniapp_name = "AUTH"
    main_action = "ol"
    middle_action = "br"
    end_action = "logoff"
    # insertOneDoc(date, time, membership_number, session_id, miniapp_name, main_action, middle_action, end_action)

    # # Fetch All Entries
    # entries = fetchAllEntries()
    # for entry in entries:
    #     print(entry)

    """
        - Find a particular customer's journey
            - If irrespective of session_id, give only membership_number in function call
            - If query is for a specific session, given membership_number and session_id
    """
    customer_journeys = findParticularCustomerJourneys(membership_number = "2", session_id = "102")
    for journey in customer_journeys:
        for path in journey:
            print(path)