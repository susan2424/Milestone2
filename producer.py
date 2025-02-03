from google.cloud import pubsub_v1  # pip install google-cloud-pubsub
import glob  # scans the json file in the directory
import json
import os
import csv

#Search for the JSON service account key
files = glob.glob("*.json")
if files:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]
else:
    print("JSON missing")
    exit()

#project id and topic name
project_id = "koobe-449321"
topic_name = "csvTopic"

#Creates the publisher and topic name for it 
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Publishing messages to {topic_path}.")

#file_path to the CSV file
file_path = "Labels.csv"

#Alters the data to from CSV to JSON
def convert_value(value):
    """Convert CSV values to int, float, or keep as string."""
    try:
        if "." in value:  # decimals are floats
            return float(value)
        return int(value)  # anything else to ints (including empty strings)
    except ValueError:
        return value  # return the string if unsuccesful conversion (e.g. for empty strings)

#To Open then Read the CSV File
with open(file_path, mode='r') as csv_file:
    reader = csv.DictReader(csv_file)

    for row in reader:
        # Convert each value in the row to the correct type
        converted_row = {key: convert_value(value) for key, value in row.items()}

        # Serialize the row to JSON
        message = json.dumps(converted_row).encode('utf-8')

        #outputs the message
        print("Publishing record:", message)
        # give the message to the publisher
        future = publisher.publish(topic_path, message)

        # confirms the publishg of the message
        future.result()

print("All records have been published.")