from pymongo import MongoClient
from datetime import datetime
import urllib 

"""
    - Mapping of start URLs and end URLs.
    - List of possible startURLs and EndURLs.
"""

endPointMappings = {
    "/ol/br/home": ["/ol/transaction/pay", "/ol/br/logoff"],
    # "/ol/transaction/pay": ["/ol/br/home", "/ol/br/logoff"],
    "/ol/transaction/pay" : ["/ol/br/home"],
    "/ol/br/logoff": ["/ol/br/login"]
}

endURLs = ["/ol/transaction/pay", "/ol/br/logoff", "/ol/br/home"]
startURLs = ["/ol/transaction/pay", "/ol/br/logoff", "/ol/br/home"]

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


"""
    - Combine the Main_Action, Middle_Action and End_Action.
"""
def extractFullAction(customer_paths, path_num):
    full_action = "/" + customer_paths[path_num]["Main_Action"] + "/" +  customer_paths[path_num]["Middle_Action"] + "/" + customer_paths[path_num]["End_Action"]
    return full_action

"""
    Given a particular customer's journeys, identify the successful journeys
"""
def extractSuccessfulJourneys(customer_paths):

    completed_journeys = []
    incomplete_journeys = []

    for path_num in range(len(customer_paths)):
        start_path = extractFullAction(customer_paths, path_num)

        if (start_path in startURLs):
            print("Start URL:", start_path)

            for next_URL_index in range(path_num + 1, len(customer_paths)):
                next_end_point = extractFullAction(customer_paths, next_URL_index)
                journey_name = customer_paths[path_num + 1]["Mini_App_Name"]
                journey_timestamp = customer_paths[path_num + 1]["Date_Time"]
                # print(journey_name)
                print("Next URL:", next_end_point)

                # If next end point is a logical next URL
                if (next_end_point in endPointMappings[start_path]):
                    completed_journeys.append([journey_name, journey_timestamp, start_path, next_end_point])
                    break

                # If next end point is not a logical next URL => Journey Incomplete
                elif (next_end_point in endURLs):
                    incomplete_journeys.append([journey_name, journey_timestamp, start_path, next_end_point])
                    break

    return completed_journeys, incomplete_journeys
    

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
    time = "12:14:05"
    membership_number = "3"
    session_id = "103"
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
    customer_journeys = findParticularCustomerJourneys(membership_number = "3", session_id = "103")
    customer_paths = []
    for journey in customer_journeys:
        for path in journey:
            customer_paths.append(path)
            print(path)
    completed_journeys, incomplete_journeys = extractSuccessfulJourneys(customer_paths)
    print("Completed Journeys:", completed_journeys)
    print("Incomplete Journeys:", incomplete_journeys)