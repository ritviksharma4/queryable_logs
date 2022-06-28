from pymongo import MongoClient
from datetime import datetime
import urllib 

"""
    - Mapping of start URLs and end URLs.
    - List of possible startURLs and EndURLs.
    - List of possible journeys.
"""

endPointMappings = {
    "/ol/br/home": ["/ol/transaction/pay", "/ol/br/logoff"],
    # "/ol/transaction/pay": ["/ol/br/home", "/ol/br/logoff"],
    "/ol/transaction/pay" : ["/ol/br/home"],
    "/ol/br/logoff": ["/ol/br/login"]
}

endURLs = ["/ol/transaction/pay", "/ol/br/logoff", "/ol/br/home"]
startURLs = ["/ol/transaction/pay", "/ol/br/logoff", "/ol/br/home"]
service_request_stats = {"AUTH" : [0, 0], "PAYMENT" : [0, 0]} 

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
def insertOneDoc(date, time, membership_number, session_id, miniapp_name, action):
    year, month, day, hour, minutes, seconds = extractDateTime(date, time)
    date_time = datetime(year, month, day, hour, minutes, seconds)
    entry = createDictionary(date_time, membership_number, session_id, miniapp_name, action)
    mongo_collection.insert_one(entry)


"""
    - Returns a JSON object containing each parameter and it's value as a key-value pair.
"""
def createDictionary(date_time, membership_number, session_id, miniapp_name, action):
    entry = {
        "Date_Time" : date_time,
        "Membership_Number" : membership_number,
        "Session_ID" : session_id,
        "Mini_App_Name" : miniapp_name,
        "Action" : action,
    }
    return entry

"""
    - Find the number of successful and unsuccessful journeys of each journey type.
"""
def findservice_request_statsJourneyFrequency(completed_journeys, incomplete_journeys):
    
    for event in completed_journeys:
        service_request_stats[event[0]][0] += 1
    for event in incomplete_journeys:
        service_request_stats[event[0]][1] += 1
    return service_request_stats


"""
    - Find Completion Rate of each Journey.
"""
def findCompletionRate(service_request_stats):

    journey_stats = []
    for mini_app in service_request_stats.keys():
        completion_rate = service_request_stats[mini_app][0] / (service_request_stats[mini_app][0] + service_request_stats[mini_app][1]) * 100
        journey_stats.append([mini_app, completion_rate])

    return journey_stats

"""
    Given a particular customer's journeys, identify the successful journeys
"""
def extractJourneysAndFreq(customer_paths):

    completed_journeys = []
    incomplete_journeys = []
    action_freq = {}

    for path_num in range(len(customer_paths)):
        # start_path = extractFullAction(customer_paths, path_num)
        start_path = customer_paths[path_num]["Action"]
        if (start_path in startURLs):
            print("Start URL:", start_path)

            for next_URL_index in range(path_num + 1, len(customer_paths)):
                # next_end_point = extractFullAction(customer_paths, next_URL_index)
                next_end_point = customer_paths[next_URL_index]["Action"]
                journey_name = customer_paths[path_num + 1]["Mini_App_Name"]
                journey_begin_timestamp = customer_paths[path_num]["Date_Time"]
                # print(journey_name)
                print("Next URL:", next_end_point)

                # Calculate frequency of every action taken by customer.
                if (next_end_point not in action_freq.keys()):
                        action_freq[next_end_point] = 1
                else:
                    action_freq[next_end_point] += 1

                # If next end point is a logical next URL
                if (next_end_point in endPointMappings[start_path]):
                    journey_end_timestamp = customer_paths[next_URL_index]["Date_Time"]
                    completed_journeys.append([journey_name, [journey_begin_timestamp, start_path], [journey_end_timestamp, next_end_point]])
                    break

                # If next end point is not a logical next URL => Journey Incomplete
                elif (next_end_point in endURLs):
                    journey_end_timestamp = customer_paths[next_URL_index]["Date_Time"]
                    incomplete_journeys.append([journey_name, [journey_begin_timestamp, start_path], [journey_end_timestamp, next_end_point]])
                    break

    return completed_journeys, incomplete_journeys, action_freq
    

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
    mongo_collection = client_logs_db.banking_logs_full_actions

    # Sample Insertion
    date = "28-05-2022"
    time = "7:19:55"
    membership_number = "1"
    session_id = "101"
    miniapp_name = "AUTH"
    action = "/ol/br/logoff"

    # insertOneDoc(date, time, membership_number, session_id, miniapp_name, action)

    # # Fetch All Entries
    # entries = fetchAllEntries()
    # for entry in entries:
    #     print(entry)

    """
        - Find a particular customer's journey
            - If irrespective of session_id, give only membership_number in function call
            - If query is for a specific session, given membership_number and session_id
    """
    customer_journeys = findParticularCustomerJourneys(membership_number = "1", session_id = "101")
    customer_paths = []
    for journey in customer_journeys:
        for path in journey:
            customer_paths.append(path)
            print(path)
    # print(customer_paths)

    completed_journeys, incomplete_journeys, action_freq = extractJourneysAndFreq(customer_paths)
    print("\nCompleted Journeys:", completed_journeys)
    print("\nIncomplete Journeys:", incomplete_journeys)
    print("\nAction Freq:", action_freq)

    service_request_stats = findservice_request_statsJourneyFrequency(completed_journeys, incomplete_journeys)
    print("\nService Request Stats:", service_request_stats)

    journey_stats = findCompletionRate(service_request_stats)
    print("\nJourney Stats:", journey_stats)